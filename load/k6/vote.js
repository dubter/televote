// Нагрузочный профиль приёма голосов.
//
// Тест меряет не «сколько держит ноутбук», а СТОИМОСТЬ ОДНОГО ГОЛОСА:
// абсолютный RPS на машине разработчика ничего не говорит о проде, а стоимость
// единицы работы масштабируется линейно и превращается в число нод.
//
// Второе, что он проверяет, важнее латентности: сумма счётчиков после дренажа
// обязана сойтись с числом принятых голосов. Потерю в пайплайне «приём → Kafka
// → консьюмер → Redis» тест на латентность не увидел бы никогда.

import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Trend } from "k6/metrics";
import exec from "k6/execution";

const BASE = __ENV.BASE_URL || "http://lb:8080";
const SLUG = __ENV.SLUG || "loadtest";
const ADMIN_LOGIN = __ENV.ADMIN_LOGIN || "admin";
const ADMIN_PASSWORD = __ENV.ADMIN_PASSWORD || "dev-only-change-me";
const INSTANCES = (__ENV.INSTANCES || "app-1:8080,app-2:8080").split(",");

const accepted = new Counter("votes_accepted");
const rejected = new Counter("votes_rejected");
const rateLimited = new Counter("votes_rate_limited");
const voteLatency = new Trend("vote_latency", true);
// drained — сколько голосов реально доехало до результатов после дренажа.
const drained = new Counter("votes_drained");

export const options = {
  scenarios: {
    // Форма нагрузки повторяет эфир: резкий всплеск, а не плавный рампап.
    // Зрители сканируют QR сразу, как он появился на экране.
    broadcast: {
      executor: "ramping-arrival-rate",
      startRate: 50,
      timeUnit: "1s",
      preAllocatedVUs: 50,
      maxVUs: Number(__ENV.MAX_VUS || 300),
      stages: [
        { target: Number(__ENV.PEAK_RPS || 500), duration: "10s" },
        { target: Number(__ENV.PEAK_RPS || 500), duration: "30s" },
        { target: 0, duration: "5s" },
      ],
    },
  },
  // По умолчанию k6 не считает p(50) и p(99), а итог обещает именно их.
  summaryTrendStats: ["min", "med", "p(50)", "p(90)", "p(95)", "p(99)", "max", "avg"],
  thresholds: {
    // Порог на приём, а не на подсчёт: подсчёт асинхронный по построению.
    "http_req_duration{scenario:broadcast}": ["p(95)<200"],
    votes_rejected: ["count<1"],
  },
};

function adminToken() {
  const r = http.post(`${BASE}/api/v1/admin/login`, JSON.stringify({
    login: ADMIN_LOGIN, password: ADMIN_PASSWORD,
  }), { headers: { "Content-Type": "application/json" } });

  if (r.status !== 200) throw new Error(`вход в админку: ${r.status} ${r.body}`);
  return r.json("token");
}

export function setup() {
  const token = adminToken();
  const auth = { headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` } };

  const opens = new Date(Date.now() - 60_000).toISOString();
  const closes = new Date(Date.now() + 3600_000).toISOString();

  http.post(`${BASE}/api/v1/admin/polls`, JSON.stringify({
    slug: SLUG,
    question: "нагрузочный опрос",
    type: "single",
    options: ["А", "Б", "В"],
    opens_at: opens,
    closes_at: closes,
    expected_audience: 100000000,
    expected_conversion: 0.3,
  }), auth);

  http.post(`${BASE}/api/v1/admin/polls/${SLUG}/open`, null, auth);

  // Конфиг разъезжается по инстансам фоновым рефрешером, поэтому ждём КАЖДЫЙ
  // инстанс поимённо, а не балансировщик: тот отдаёт 200 уже с одной реплики,
  // и первые голоса, попавшие на вторую, получают 404. Тест объявил бы это
  // потерей голосов, которой нет.
  for (const host of INSTANCES) {
    const deadline = Date.now() + 30_000;
    let ready = false;
    while (Date.now() < deadline) {
      if (http.get(`http://${host}/api/v1/polls/${SLUG}`).status === 200) {
        ready = true;
        break;
      }
      sleep(0.25);
    }
    if (!ready) throw new Error(`инстанс ${host} не увидел опрос ${SLUG}`);
  }
  return { token };
}

export default function () {
  // Уникальный голосующий на итерацию: цель — померить приём, а не дедуп.
  // Повторы дали бы already_counted и занизили полезную работу.
  const voter = `k6-${exec.scenario.iterationInTest}-${__VU}`;

  const res = http.post(
    `${BASE}/api/v1/polls/${SLUG}/vote`,
    JSON.stringify({ choices: [exec.scenario.iterationInTest % 3], voter }),
    { headers: { "Content-Type": "application/json" }, tags: { name: "vote" } },
  );

  voteLatency.add(res.timings.duration);

  if (res.status === 202) accepted.add(1);
  else if (res.status === 429) rateLimited.add(1);
  else rejected.add(1);

  check(res, { "голос принят (202)": (r) => r.status === 202 });
}

export function teardown(data) {
  const auth = { headers: { Authorization: `Bearer ${data.token}` } };

  // Ждём конца дренажа: приём закончился, подсчёт продолжается.
  //
  // Интервал опроса ЗАВЕДОМО БОЛЬШЕ интервала снапшотера. Опрос чаще давал бы
  // три одинаковых чтения внутри одного цикла снапшота, и проверка объявила бы
  // дренаж законченным раньше времени — то есть сообщила бы о потере голосов,
  // которой нет. Тест, врущий про потерю, хуже отсутствующего теста.
  const pollInterval = Number(__ENV.DRAIN_POLL_SECONDS || 5);
  const deadline = Date.now() + 120_000;

  let ballots = 0;
  let stable = 0;

  while (Date.now() < deadline) {
    sleep(pollInterval);

    const r = http.get(`${BASE}/api/v1/admin/polls/${SLUG}/results`, auth);
    if (r.status !== 200) continue;

    const next = r.json("ballots");
    stable = next === ballots && next > 0 ? stable + 1 : 0;
    ballots = next;

    if (stable >= 2) break;
  }

  drained.add(ballots);
  console.log(`\n  Дренаж завершён: ${ballots} бюллетеней в результатах\n`);
}

export function handleSummary(data) {
  const m = data.metrics;
  const acceptedCount = m.votes_accepted ? m.votes_accepted.values.count : 0;
  const limited = m.votes_rate_limited ? m.votes_rate_limited.values.count : 0;
  const failed = m.votes_rejected ? m.votes_rejected.values.count : 0;
  const dur = m.http_req_duration.values;
  const rps = m.http_reqs ? m.http_reqs.values.rate : 0;

  const counted = m.votes_drained ? m.votes_drained.values.count : 0;
  const lost = acceptedCount - counted;
  const verdict = lost === 0
    ? "сходится — потерь в пайплайне нет"
    : `РАСХОЖДЕНИЕ ${lost} (${((lost / Math.max(acceptedCount, 1)) * 100).toFixed(3)} %)`;

  const lines = [
    "",
    "  ── Корректность ────────────────────────────────────────────",
    `  принято на входе       ${acceptedCount}`,
    `  посчитано после дренажа ${counted}`,
    `  сверка                 ${verdict}`,
    "",
    "  Это главная проверка теста: потерю в пайплайне приём → Kafka →",
    "  консьюмер → Redis тест на латентность не увидел бы никогда.",
    "",
    "  ── Стоимость приёма одного голоса ──────────────────────────",
    `  принято 202            ${acceptedCount}`,
    `  отвергнуто (429)       ${limited}`,
    `  ошибок                 ${failed}`,
    `  достигнутый RPS        ${rps.toFixed(0)}`,
    `  латентность p50/p95/p99  ${dur["p(50)"].toFixed(1)} / ${dur["p(95)"].toFixed(1)} / ${dur["p(99)"].toFixed(1)} мс`,
    "",
    "  Абсолютный RPS на этой машине не переносится в прод.",
    "  Переносится стоимость единицы работы: латентность приёма и",
    "  доля отказов при известной ёмкости — из них ёмкость под 2M RPS",
    "  считается линейно (docs/design.md §2).",
    "",
  ];

  return { stdout: lines.join("\n") };
}
