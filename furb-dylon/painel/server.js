import express from 'express';
import Docker from 'dockerode';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import {
  SQSClient, GetQueueUrlCommand, GetQueueAttributesCommand, SendMessageCommand, PurgeQueueCommand,
} from '@aws-sdk/client-sqs';
import { NodeHttpHandler } from '@smithy/node-http-handler';

const SVC = {
  inscricoes: process.env.INSCRICOES_URL ?? 'http://inscricoes:8080',
  coletor: process.env.COLETOR_URL ?? 'http://coletor-dion:8080',
  notas: process.env.NOTAS_URL ?? 'http://notas:8080',
  notificacao: process.env.NOTIFICACAO_URL ?? 'http://notificacao:8080',
};
const PROJETO = process.env.COMPOSE_PROJECT ?? 'furb-dylon';
const FILAS = ['inscricoes', 'coletor', 'notas', 'notificacao'];

const sqs = new SQSClient({ requestHandler: new NodeHttpHandler({ connectionTimeout: 2000, requestTimeout: 3000 }) });
const docker = new Docker({ socketPath: '/var/run/docker.sock' });
const app = express();
app.use(express.json());
app.use(express.static(fileURLToPath(new globalThis.URL('./public', import.meta.url))));
const log = (...a) => console.log(new Date().toISOString(), '[painel]', ...a);
const dormir = (ms) => new Promise((r) => setTimeout(r, ms));

async function http(metodo, url, corpo, timeoutMs = 8000) {
  const resp = await fetch(url, {
    method: metodo,
    headers: corpo ? { 'Content-Type': 'application/json' } : {},
    body: corpo ? JSON.stringify(corpo) : undefined,
    signal: AbortSignal.timeout(timeoutMs),
  });
  const texto = await resp.text();
  let dados; try { dados = JSON.parse(texto); } catch { dados = texto; }
  return { status: resp.status, dados };
}
const tentar = async (fn) => { try { return await fn(); } catch (e) { return { erro: e.message }; } };

const urls = {};
async function urlFila(nome) {
  urls[nome] ??= (await sqs.send(new GetQueueUrlCommand({ QueueName: nome }))).QueueUrl;
  return urls[nome];
}
async function profundidade(nome) {
  const r = await sqs.send(new GetQueueAttributesCommand({
    QueueUrl: await urlFila(nome),
    AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
  }));
  return { visiveis: Number(r.Attributes.ApproximateNumberOfMessages), emProcessamento: Number(r.Attributes.ApproximateNumberOfMessagesNotVisible) };
}
const envelope = (type, correlationId, payload, messageId = randomUUID()) => ({
  messageId, correlationId, type, version: 1, occurredAt: new Date().toISOString(), payload,
});
async function enviar(fila, corpo) {
  await sqs.send(new SendMessageCommand({ QueueUrl: await urlFila(fila), MessageBody: typeof corpo === 'string' ? corpo : JSON.stringify(corpo) }));
}

async function conteineres() {
  const lista = await docker.listContainers({ all: true, filters: { label: [`com.docker.compose.project=${PROJETO}`] } });
  return lista.map((c) => ({
    id: c.Id.slice(0, 12),
    nome: c.Names[0].replace(/^\//, ''),
    servico: c.Labels['com.docker.compose.service'],
    estado: c.State,
    status: c.Status,
  })).sort((a, b) => a.nome.localeCompare(b.nome));
}
async function doServico(servico) {
  return (await conteineres()).filter((c) => c.servico === servico);
}
function demux(buf) {
  const linhas = [];
  let i = 0;
  while (i + 8 <= buf.length) {
    const tam = buf.readUInt32BE(i + 4);
    linhas.push(buf.subarray(i + 8, i + 8 + tam).toString('utf8'));
    i += 8 + tam;
  }
  return linhas.join('');
}

app.get('/api/estado', async (_req, res) => {
  const [inscricoes, notas, coletor, emails, filas, docker_] = await Promise.all([
    tentar(async () => (await http('GET', `${SVC.inscricoes}/admin/estado`, null, 3000)).dados),
    tentar(async () => (await http('GET', `${SVC.notas}/admin/estado`, null, 3000)).dados),
    tentar(async () => (await http('GET', `${SVC.coletor}/admin/estado`, null, 3000)).dados),
    tentar(async () => (await http('GET', `${SVC.notificacao}/emails`, null, 3000)).dados),
    tentar(async () => {
      const out = Object.fromEntries(FILAS.map((f) => [f, null]));
      await Promise.all(FILAS.map(async (f) => {
        out[f] = { ...(await profundidade(f)), dlq: (await profundidade(`${f}-dlq`)).visiveis };
      }));
      return out;
    }),
    tentar(conteineres),
  ]);
  if (docker_.erro) log(`sem acesso ao Docker: ${docker_.erro}`);
  res.json({ inscricoes, notas, coletor, emails, filas, conteineres: docker_ });
});

let logsDesde = 0;
app.post('/api/logs/zerar', (_req, res) => {
  logsDesde = Math.floor(Date.now() / 1000);
  res.json({ ok: true });
});

app.get('/api/logs/:servico', async (req, res) => {
  try {
    const cs = await doServico(req.params.servico);
    const partes = await Promise.all(cs.map(async (c) => {
      const buf = await docker.getContainer(c.id).logs({ stdout: true, stderr: true, tail: Number(req.query.tail ?? 60), since: logsDesde });
      return demux(buf).split('\n').filter(Boolean).map((l) => (cs.length > 1 ? `[${c.nome}] ${l}` : l));
    }));
    res.json({ linhas: partes.flat().slice(-200) });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.post('/api/inscricoes', async (req, res) => {
  const r = await tentar(() => http('POST', `${SVC.inscricoes}/inscricoes`, req.body));
  res.status(r.status ?? 502).json(r.dados ?? r);
});

app.post('/api/login', async (req, res) => {
  const r = await tentar(() => http('POST', `${SVC.inscricoes}/login`, req.body));
  res.status(r.status ?? 502).json(r.dados ?? r);
});

app.get('/api/inscricoes/:id', async (req, res) => {
  const r = await tentar(() => http('GET', `${SVC.inscricoes}/inscricoes/${encodeURIComponent(req.params.id)}`));
  res.status(r.status ?? 502).json(r.dados ?? r);
});

app.get('/api/vagas', async (_req, res) => {
  const r = await tentar(() => http('GET', `${SVC.inscricoes}/vagas`));
  res.status(r.status ?? 502).json(r.dados ?? r);
});

app.post('/api/demo/reset', async (_req, res) => {
  const r = await Promise.all([
    tentar(() => http('POST', `${SVC.inscricoes}/admin/reset`)),
    tentar(() => http('POST', `${SVC.notas}/admin/reset`)),
    tentar(() => http('POST', `${SVC.coletor}/admin/reset`)),
    tentar(() => http('POST', `${SVC.notificacao}/admin/reset`)),
    tentar(() => http('POST', `${SVC.coletor}/admin/dion`, { latencia_ms: 500, taxa_falha: 0 })),
    ...FILAS.flatMap((f) => [f, `${f}-dlq`]).map((f) => tentar(async () => sqs.send(new PurgeQueueCommand({ QueueUrl: await urlFila(f) })))),
  ]);
  log('demo zerada');
  res.json({ ok: true, detalhes: r.map((x) => x.status ?? x.erro ?? 'ok') });
});

app.post('/api/demo/coletar', async (_req, res) => {
  const r = await tentar(() => http('POST', `${SVC.coletor}/coletar`, null, 60000));
  res.json(r.dados ?? r);
});

app.post('/api/demo/esquecer', async (req, res) => {
  const r = await tentar(() => http('POST', `${SVC.notas}/admin/esquecer`, req.body));
  res.json(r.dados ?? r);
});

app.post('/api/demo/ao-vivo', async (_req, res) => {
  const est = await tentar(async () => (await http('GET', `${SVC.coletor}/admin/estado`)).dados);
  const alunos = (est.alunos ?? []).map((a) => a.matricula);
  if (alunos.length === 0) return res.json({ erro: 'Nenhum aluno inscrito ainda. Faça uma inscrição primeiro.' });
  let apagadas = 0;
  for (const matricula of alunos) {
    const r = await tentar(async () => (await http('POST', `${SVC.notas}/admin/esquecer`, { matricula, quantidade: 2 })).dados);
    apagadas += r.apagadas ?? 0;
  }
  const c = await tentar(async () => (await http('POST', `${SVC.coletor}/coletar`, null, 120000)).dados);
  res.json({ alunos, apagadas, raspados: c.alunos ?? 0 });
});

async function alunoInscrito() {
  const est = await tentar(async () => (await http('GET', `${SVC.coletor}/admin/estado`)).dados);
  return est.alunos?.[0]?.matricula;
}

app.post('/api/dion', async (req, res) => {
  const r = await tentar(() => http('POST', `${SVC.coletor}/admin/dion`, req.body));
  res.json(r.dados ?? r);
});

app.post('/api/religar', async (_req, res) => {
  const parados = (await conteineres()).filter((c) => c.estado !== 'running' && c.servico !== 'aws-init');
  for (const c of parados) {
    const ct = docker.getContainer(c.id);
    if (c.estado === 'paused') await ct.unpause(); else await ct.start();
  }
  res.json({ religados: parados.map((c) => c.nome) });
});

app.post('/api/conteineres/:nome/:acao', async (req, res) => {
  const c = (await conteineres()).find((x) => x.nome === req.params.nome);
  if (!c || c.servico === 'aws-init' || c.servico === 'painel') return res.status(404).json({ erro: 'contêiner não encontrado' });
  const ct = docker.getContainer(c.id);
  try {
    if (req.params.acao === 'desligar') {
      if (c.estado === 'paused') await ct.unpause();
      await ct.stop({ t: 2 });
    } else if (req.params.acao === 'ligar') {
      if (c.estado === 'paused') await ct.unpause(); else await ct.start();
    } else {
      return res.status(400).json({ erro: 'ação inválida' });
    }
    res.json({ ok: true, nome: c.nome, acao: req.params.acao });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

function lote(n, prefixo) {
  return Array.from({ length: n }, (_, i) => {
    const matricula = `${prefixo}-${String(i + 1).padStart(2, '0')}`;
    return envelope('notas.snapshot', randomUUID(), {
      matricula, coletaId: randomUUID(),
      notas: [{ disciplina: 'Sistemas Distribuídos', avaliacao: 'P1', valor: 5 + (i % 50) / 10 }],
    });
  });
}

const cenarios = {
  async F1({ matricula = 'f1.aluno', email = 'f1.aluno@teste.invalid' }) {
    await http('POST', `${SVC.coletor}/admin/dion`, { taxa_falha: 1 });
    const r = await http('POST', `${SVC.inscricoes}/inscricoes`, { matricula, senha: 'boa', email });
    return { passo: 'DION com 100% de falha; inscrição enviada. Acompanhe: VALIDANDO -> RECUSADA (DION_INDISPONIVEL), vaga volta, e-mail de recusa.', inscricao: r.dados };
  },

  async F2() {
    const msgs = lote(30, 'carga');
    await Promise.all(msgs.map((m) => enviar('notas', m)));
    await dormir(1200);
    const vivas = (await doServico('notas')).filter((c) => c.estado === 'running');
    if (vivas.length === 0) return { erro: 'nenhuma réplica do notas rodando' };
    const alvo = vivas[Math.floor(Math.random() * vivas.length)];
    await docker.getContainer(alvo.id).kill();
    log(`F2: docker kill ${alvo.nome}`);
    return { passo: `30 snapshots enviados e ${alvo.nome} morto no meio. Mensagens em processamento voltam em ~10s para outra réplica. Confira "snapshots por réplica" = 30 no total.`, morto: alvo.nome };
  },

  async F3({ matricula }) {
    matricula ??= await alunoInscrito();
    if (!matricula) return { erro: 'Nenhum aluno inscrito ainda. Faça uma inscrição primeiro.' };
    const disciplina = `Prova duplicada ${new Date().toLocaleTimeString('pt-BR')}`;
    const m = envelope('notas.snapshot', randomUUID(), {
      matricula, coletaId: randomUUID(), notas: [{ disciplina, avaliacao: 'P1', valor: 9.5 }],
    });
    for (let i = 0; i < 5; i++) await enviar('notas', m);
    return { passo: `Mesmo messageId (${m.messageId.slice(0, 8)}) enviado 5x. Resultado esperado: 1 nota "${disciplina}" e 1 e-mail.`, messageId: m.messageId };
  },

  async F4() {
    await enviar('notas', '{ isso não é JSON');
    const ok = lote(3, 'pos-veneno');
    for (const m of ok) await enviar('notas', m);
    return { passo: 'Mensagem venenosa + 3 válidas na fila "notas". As válidas são processadas na hora; a venenosa vai para notas-dlq depois de 3 tentativas (~30s).' };
  },

  async F5() {
    await Promise.all([
      http('POST', `${SVC.inscricoes}/admin/reset`),
      http('POST', `${SVC.coletor}/admin/dion`, { taxa_falha: 0, latencia_ms: 300 }),
    ]);
    const inicio = Date.now();
    const respostas = await Promise.all(Array.from({ length: 100 }, (_, i) => {
      const n = String(i + 1).padStart(3, '0');
      return tentar(() => http('POST', `${SVC.inscricoes}/inscricoes`, { matricula: `f5-${n}`, senha: 'boa', email: `f5-${n}@teste.invalid` }, 20000));
    }));
    const contagem = {};
    for (const r of respostas) {
      const k = r.dados?.status ?? `HTTP ${r.status ?? 'erro'}`;
      contagem[k] = (contagem[k] ?? 0) + 1;
    }
    return { passo: `100 inscrições disparadas ao mesmo tempo (${Date.now() - inicio} ms). Esperado: exatamente 10 VALIDANDO e 90 RECUSADA.`, contagem };
  },

  async F6({ segundos = 30 }) {
    const [ms] = await doServico('ministack');
    if (!ms) return { erro: 'contêiner do ministack não encontrado' };
    const msgs = lote(6, 'queda');
    await Promise.all(msgs.map((m) => enviar('notas', m)));
    await dormir(300);
    await docker.getContainer(ms.id).pause();
    log(`F6: ministack pausado por ${segundos}s`);
    setTimeout(async () => {
      await tentar(() => docker.getContainer(ms.id).unpause());
      log('F6: ministack de volta');
    }, segundos * 1000);
    return { passo: `MiniStack pausado por ${segundos}s. Veja os logs ("broker indisponível"), a outbox acumulando e, quando voltar, os serviços reconectando sozinhos e a outbox drenando.`, voltaEm: segundos };
  },

  async F7({ matricula = 'f7.aluno', email = 'f7.aluno@teste.invalid', latenciaMs = 25000 }) {
    await http('POST', `${SVC.coletor}/admin/dion`, { latencia_ms: latenciaMs, taxa_falha: 0 });
    const r = await http('POST', `${SVC.inscricoes}/inscricoes`, { matricula, senha: 'boa', email });
    return { passo: `DION com ~${latenciaMs / 1000}s de latência (timeout da saga: 15s). Esperado: RECUSADA por TIMEOUT, vaga liberada; quando o DION responder, o log do inscricoes mostra "resposta tardia, descartada".`, inscricao: r.dados };
  },

  async F8() {
    const msgs = lote(30, 'escala');
    await Promise.all(msgs.map((m) => enviar('notas', m)));
    const replicas = (await doServico('notas')).map((c) => `${c.nome} (${c.id})`);
    return { passo: '30 snapshots enviados. Veja "snapshots por réplica": a carga se divide entre as réplicas, sem nenhum processado duas vezes.', replicas };
  },
};

app.post('/api/falhas/:id', async (req, res) => {
  const fn = cenarios[req.params.id.toUpperCase()];
  if (!fn) return res.status(404).json({ erro: 'cenário desconhecido' });
  log(`cenário ${req.params.id} disparado`);
  res.json(await tentar(() => fn(req.body ?? {})));
});

app.get('/health', (_req, res) => res.json({ status: 'UP', servico: 'painel' }));
app.listen(Number(process.env.PORT ?? 8080), () => log('painel em http://localhost:8080'));
