const JSON_HEADERS = { "Content-Type": "application/json" };

function resolveUrl(baseUrl, path) {
  return `${baseUrl.replace(/\/+$/, "")}${path}`;
}

async function readJson(response) {
  const text = await response.text();
  if (!text) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch (failure) {
    throw new Error(`Invalid JSON response: ${failure.message}`);
  }
}

async function request(url, options) {
  const response = await fetch(url, options);
  const data = await readJson(response);
  if (!response.ok && response.status !== 202) {
    const message = data?.error ?? `Request failed (${response.status})`;
    throw new Error(message);
  }
  return { status: response.status, data };
}

function putIfPresent(target, key, value) {
  if (value != null && value !== "") {
    if (target instanceof FormData) {
      target.append(key, String(value));
      return;
    }
    target[key] = value;
  }
}

export function postScanWithPath({ baseUrl, path, async }) {
  const payload = { path };
  putIfPresent(payload, "async", async);
  return request(resolveUrl(baseUrl, "/api/scan"), {
    method: "POST",
    headers: JSON_HEADERS,
    body: JSON.stringify(payload)
  });
}

export function postScanWithUpload({ baseUrl, file, async }) {
  const formData = new FormData();
  formData.append("file", file);
  putIfPresent(formData, "async", String(Boolean(async)));
  return request(resolveUrl(baseUrl, "/api/scan"), {
    method: "POST",
    body: formData
  });
}

export function postPrepare({ baseUrl, inputFolder, expectedFolder, scenarioPath, cacheDir, async }) {
  const payload = { inputFolder, expectedFolder, scenarioPath };
  putIfPresent(payload, "cacheDir", cacheDir);
  putIfPresent(payload, "async", async);
  return request(resolveUrl(baseUrl, "/api/prepare"), {
    method: "POST",
    headers: JSON_HEADERS,
    body: JSON.stringify(payload)
  });
}

export function postRunOffline({ baseUrl, scenarioPath, junitPath, async }) {
  const payload = { scenarioPath };
  putIfPresent(payload, "junitPath", junitPath);
  putIfPresent(payload, "async", async);
  return request(resolveUrl(baseUrl, "/api/run-offline"), {
    method: "POST",
    headers: JSON_HEADERS,
    body: JSON.stringify(payload)
  });
}

export function postRunOnline({
  baseUrl,
  scenarioPath,
  transportClass,
  transportProperties,
  transportConfigPath,
  receiveTimeoutMs,
  queueCapacity,
  junitPath,
  async
}) {
  const payload = { scenarioPath, transportClass };
  putIfPresent(payload, "transportProperties", transportProperties);
  putIfPresent(payload, "transportConfigPath", transportConfigPath);
  if (Number.isFinite(receiveTimeoutMs)) {
    payload.receiveTimeoutMs = receiveTimeoutMs;
  }
  if (Number.isFinite(queueCapacity)) {
    payload.queueCapacity = queueCapacity;
  }
  putIfPresent(payload, "junitPath", junitPath);
  putIfPresent(payload, "async", async);
  return request(resolveUrl(baseUrl, "/api/run-online"), {
    method: "POST",
    headers: JSON_HEADERS,
    body: JSON.stringify(payload)
  });
}

export function getJob({ baseUrl, jobId }) {
  return request(resolveUrl(baseUrl, `/api/job/${encodeURIComponent(jobId)}`), {
    method: "GET"
  });
}

export async function pollJob({ baseUrl, jobId, onUpdate, timeoutMs = 120_000, pollIntervalMs = 700 }) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const snapshot = await getJob({ baseUrl, jobId });
    if (onUpdate) {
      onUpdate(snapshot.data);
    }
    const status = snapshot.data?.status;
    if (status === "SUCCEEDED" || status === "FAILED") {
      return snapshot.data;
    }
    await new Promise(resolve => setTimeout(resolve, pollIntervalMs));
  }
  throw new Error(`Job polling timed out for job ${jobId}`);
}
