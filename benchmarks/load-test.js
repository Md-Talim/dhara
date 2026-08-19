import { check } from "k6";
import http from "k6/http";
import { Trend } from "k6/metrics";

const enqueueLatency = new Trend("dhara_enqueue_latency");

export const options = {
  scenarios: {
    sustained_load: {
      executor: "constant-arrival-rate",
      rate: 100, // target near expected completion capacity
      timeUnit: "1s",
      duration: "60s",
      preAllocatedVUs: 30,
      maxVUs: 100,
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<500", "p(99)<1000"], // this still only gates enqueue latency, not completion
  },
};

export default function () {
  const payload = JSON.stringify({
    type: "realistic_work",
    payload: { message: `load-test-${__VU}-${__ITER}` },
  });

  const res = http.post("http://localhost:8080/api/v1/tasks", payload, {
    headers: { "Content-Type": "application/json" },
  });

  enqueueLatency.add(res.timings.duration);

  check(res, {
    "status is 201": (r) => r.status === 201,
  });
}
