import express from 'express';
import pg from 'pg';
import { SQSClient, GetQueueUrlCommand, ReceiveMessageCommand, DeleteMessageCommand } from '@aws-sdk/client-sqs';
import { SESClient, SendEmailCommand } from '@aws-sdk/client-ses';
import nodemailer from 'nodemailer';
import { NodeHttpHandler } from '@smithy/node-http-handler';

const FILA = process.env.QUEUE_ENTRADA ?? 'notificacao';
const REMETENTE = process.env.SES_REMETENTE ?? 'dylon@furb-dylon.dev';
const PORTA = Number(process.env.PORT ?? 8080);

const requestHandler = new NodeHttpHandler({ connectionTimeout: 3000, requestTimeout: 13000 });
const sqs = new SQSClient({ requestHandler });
const ses = new SESClient({ requestHandler });
const pool = new pg.Pool({ connectionString: process.env.DB_URL ?? 'postgresql://notificacao:notificacao@localhost:5432/notificacao' });

const smtp = process.env.SMTP_HOST
  ? nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: Number(process.env.SMTP_PORT ?? 587),
    secure: Number(process.env.SMTP_PORT ?? 587) === 465,
    auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
  })
  : null;

const log = (...a) => console.log(new Date().toISOString(), '[notificacao]', ...a);
const dormir = (ms) => new Promise((r) => setTimeout(r, ms));

async function criarTabelas() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS alunos (
      matricula TEXT PRIMARY KEY,
      email     TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS mensagens_processadas (
      message_id    UUID PRIMARY KEY,
      processado_em TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS emails_enviados (
      id             BIGSERIAL PRIMARY KEY,
      message_id     UUID NOT NULL,
      correlation_id UUID NOT NULL,
      para           TEXT NOT NULL,
      assunto        TEXT NOT NULL,
      corpo          TEXT,
      enviado_em     TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    ALTER TABLE emails_enviados ADD COLUMN IF NOT EXISTS corpo TEXT;`);
}

const MOTIVOS = {
  SEM_VAGAS: 'todas as vagas de monitoramento estão ocupadas',
  SENHA_INVALIDA: 'o DION recusou a matrícula/senha informadas',
  DION_INDISPONIVEL: 'o DION estava fora do ar na hora da validação',
  TIMEOUT: 'o DION demorou demais para responder',
};

const DESTINO_DE_TESTE = /(^(f\d|carga-|escala-|queda-|pos-veneno-))|(\.invalid$)/i;

async function enviarEmail(db, env, para, assunto, texto) {
  await ses.send(new SendEmailCommand({
    Source: REMETENTE,
    Destination: { ToAddresses: [para] },
    Message: { Subject: { Data: assunto, Charset: 'UTF-8' }, Body: { Text: { Data: texto, Charset: 'UTF-8' } } },
  }));
  const real = smtp && !DESTINO_DE_TESTE.test(para);
  if (real) {
    await smtp.sendMail({ from: process.env.SMTP_FROM || process.env.SMTP_USER, to: para, subject: assunto, text: texto });
  }
  await db.query('INSERT INTO emails_enviados (message_id, correlation_id, para, assunto, corpo) VALUES ($1, $2, $3, $4, $5)',
    [env.messageId, env.correlationId, para, assunto, texto]);
  log(`[${env.correlationId}] e-mail "${assunto}" enviado para ${para}${real ? ' (SES + SMTP real)' : ' (SES)'}`);
}

async function tratar(db, env) {
  const p = env.payload;
  switch (env.type) {
    case 'inscricao.confirmada':
      await db.query(`INSERT INTO alunos (matricula, email) VALUES ($1, $2)
                      ON CONFLICT (matricula) DO UPDATE SET email = EXCLUDED.email`, [p.matricula, p.email]);
      return enviarEmail(db, env, p.email, 'Furb Dylon: inscrição confirmada',
        `Tudo certo! A partir de agora a matrícula ${p.matricula} está sendo monitorada no DION.\n` +
        'Você recebe um e-mail sempre que sair nota nova.');
    case 'inscricao.recusada':
      return enviarEmail(db, env, p.email, 'Furb Dylon: inscrição não concluída',
        `Não foi possível concluir a inscrição da matrícula ${p.matricula}: ${MOTIVOS[p.motivo] ?? p.motivo}.`);
    case 'notas.novas': {
      const { rows } = await db.query('SELECT email FROM alunos WHERE matricula = $1', [p.matricula]);
      if (rows.length === 0) {
        log(`[${env.correlationId}] notas de ${p.matricula}, mas o aluno não tem cadastro aqui: e-mail descartado`);
        return;
      }
      const linhas = p.notas.map((n) => `- ${n.disciplina}, ${n.avaliacao}: ${Number(n.valor).toFixed(1)}`);
      const assunto = p.notas.length === 1 ? 'Saiu 1 nota nova no DION' : `Saíram ${p.notas.length} notas novas no DION`;
      return enviarEmail(db, env, rows[0].email, assunto, `Notas novas:\n\n${linhas.join('\n')}\n`);
    }
    default:
      throw new Error(`tipo inesperado: ${env.type}`);
  }
}

async function processar(corpo) {
  let env = JSON.parse(corpo);
  if (env.Type === 'Notification') env = JSON.parse(env.Message);
  if (!env.messageId || !env.type || !env.payload) throw new Error('envelope inválido');

  const db = await pool.connect();
  try {
    await db.query('BEGIN');
    const r = await db.query('INSERT INTO mensagens_processadas (message_id) VALUES ($1) ON CONFLICT DO NOTHING', [env.messageId]);
    if (r.rowCount === 0) {
      log(`mensagem ${env.messageId} já processada, ignorada (idempotência)`);
    } else {
      await tratar(db, env);
    }
    await db.query('COMMIT');
  } catch (e) {
    await db.query('ROLLBACK').catch(() => {});
    throw e;
  } finally {
    db.release();
  }
}

async function consumir() {
  let url;
  let espera = 1000;
  for (;;) {
    try {
      url ??= (await sqs.send(new GetQueueUrlCommand({ QueueName: FILA }))).QueueUrl;
      const resp = await sqs.send(new ReceiveMessageCommand({ QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 10 }));
      if (espera > 1000) log('conexão com o broker restabelecida');
      espera = 1000;
      for (const m of resp.Messages ?? []) {
        try {
          await processar(m.Body);
          await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: m.ReceiptHandle }));
        } catch (e) {
          log(`falha ao processar ${m.MessageId} (será reentregue): ${e.message}`);
        }
      }
    } catch (e) {
      log(`broker indisponível, nova tentativa em ${espera} ms (${e.name})`);
      await dormir(espera);
      espera = Math.min(espera * 2, 15000);
    }
  }
}

const app = express();
app.get('/health', (_req, res) => res.json({ status: 'UP', servico: 'notificacao' }));
app.get('/emails', async (_req, res) => {
  const { rows } = await pool.query('SELECT * FROM emails_enviados ORDER BY id DESC LIMIT 50');
  res.json(rows);
});
app.post('/admin/reset', async (_req, res) => {
  await pool.query('TRUNCATE alunos, mensagens_processadas, emails_enviados');
  log('ADMIN: banco de notificações zerado');
  res.json({ status: 'ok' });
});

for (let i = 0; ; i++) {
  try { await criarTabelas(); break; } catch (e) {
    if (i >= 30) throw e;
    log(`aguardando banco (${i})`);
    await dormir(2000);
  }
}
app.listen(PORTA, () => log(`ouvindo na porta ${PORTA}; entrega real por SMTP: ${smtp ? process.env.SMTP_HOST : 'desligada'}`));
consumir();
