import { createContext, useCallback, useContext, useMemo, useState } from "react";
import {
  pollJob,
  postPrepare,
  postRunOffline,
  postRunOnline,
  postScanWithPath,
  postScanWithUpload
} from "../api/client";
import { scenarioTemplate } from "../templates/scenarioTemplate";

const AppStateContext = createContext(null);

const defaultInputs = {
  apiBaseUrl: import.meta.env.VITE_API_BASE_URL ?? "http://localhost:7000",
  scanPath: "",
  scanFile: null,
  scanAsync: false,
  inputFolder: "",
  expectedFolder: "",
  scenarioPath: "",
  cacheDir: "",
  prepareAsync: false,
  junitPath: "",
  offlineAsync: false,
  onlineAsync: true,
  transportClass: "io.fixreplay.adapter.artio.ArtioFixTransport",
  transportConfigPath: "",
  transportPropertiesText: "",
  receiveTimeoutMs: "5000",
  queueCapacity: "1024",
  scenarioYaml: scenarioTemplate
};

export function AppStateProvider({ children }) {
  const [inputs, setInputs] = useState(defaultInputs);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [resultPayload, setResultPayload] = useState(null);
  const [lastOperation, setLastOperation] = useState(null);
  const [jobSnapshot, setJobSnapshot] = useState(null);

  const setInput = useCallback((name, value) => {
    setInputs(current => ({ ...current, [name]: value }));
  }, []);

  const parseTransportProperties = useCallback(() => {
    const text = inputs.transportPropertiesText.trim();
    if (!text) {
      return {};
    }
    try {
      const parsed = JSON.parse(text);
      if (typeof parsed !== "object" || parsed == null || Array.isArray(parsed)) {
        throw new Error("transportProperties JSON must be an object");
      }
      return parsed;
    } catch (failure) {
      throw new Error(`Invalid transportProperties JSON: ${failure.message}`);
    }
  }, [inputs.transportPropertiesText]);

  const runRequest = useCallback(
    async (operation, requestFactory) => {
      setLoading(true);
      setError("");
      setLastOperation(operation);
      setJobSnapshot(null);
      try {
        const response = await requestFactory();
        if (response.status === 202 && response.data?.jobId) {
          setJobSnapshot(response.data);
          const finalSnapshot = await pollJob({
            baseUrl: inputs.apiBaseUrl,
            jobId: response.data.jobId,
            onUpdate: snapshot => setJobSnapshot(snapshot)
          });
          setJobSnapshot(finalSnapshot);
          if (finalSnapshot.status !== "SUCCEEDED") {
            throw new Error(finalSnapshot.error ?? `Job ${finalSnapshot.status}`);
          }
          setResultPayload(finalSnapshot.result ?? null);
          return finalSnapshot.result ?? null;
        }
        setResultPayload(response.data);
        return response.data;
      } catch (failure) {
        setError(failure.message);
        throw failure;
      } finally {
        setLoading(false);
      }
    },
    [inputs.apiBaseUrl]
  );

  const runScan = useCallback(async () => {
    if (inputs.scanFile) {
      return runRequest("scan", () =>
        postScanWithUpload({
          baseUrl: inputs.apiBaseUrl,
          file: inputs.scanFile,
          async: inputs.scanAsync
        })
      );
    }
    return runRequest("scan", () =>
      postScanWithPath({
        baseUrl: inputs.apiBaseUrl,
        path: inputs.scanPath.trim(),
        async: inputs.scanAsync
      })
    );
  }, [inputs, runRequest]);

  const runPrepare = useCallback(async () => {
    return runRequest("prepare", () =>
      postPrepare({
        baseUrl: inputs.apiBaseUrl,
        inputFolder: inputs.inputFolder.trim(),
        expectedFolder: inputs.expectedFolder.trim(),
        scenarioPath: inputs.scenarioPath.trim(),
        cacheDir: inputs.cacheDir.trim(),
        async: inputs.prepareAsync
      })
    );
  }, [inputs, runRequest]);

  const runOffline = useCallback(async () => {
    return runRequest("run-offline", () =>
      postRunOffline({
        baseUrl: inputs.apiBaseUrl,
        scenarioPath: inputs.scenarioPath.trim(),
        junitPath: inputs.junitPath.trim(),
        async: inputs.offlineAsync
      })
    );
  }, [inputs, runRequest]);

  const runOnline = useCallback(async () => {
    return runRequest("run-online", () =>
      postRunOnline({
        baseUrl: inputs.apiBaseUrl,
        scenarioPath: inputs.scenarioPath.trim(),
        transportClass: inputs.transportClass.trim(),
        transportProperties: parseTransportProperties(),
        transportConfigPath: inputs.transportConfigPath.trim(),
        receiveTimeoutMs: Number.parseInt(inputs.receiveTimeoutMs, 10),
        queueCapacity: Number.parseInt(inputs.queueCapacity, 10),
        junitPath: inputs.junitPath.trim(),
        async: inputs.onlineAsync
      })
    );
  }, [inputs, parseTransportProperties, runRequest]);

  const loadScenarioTemplate = useCallback(() => {
    setInput("scenarioYaml", scenarioTemplate);
  }, [setInput]);

  const value = useMemo(
    () => ({
      inputs,
      loading,
      error,
      resultPayload,
      lastOperation,
      jobSnapshot,
      setInput,
      runScan,
      runPrepare,
      runOffline,
      runOnline,
      loadScenarioTemplate
    }),
    [
      inputs,
      loading,
      error,
      resultPayload,
      lastOperation,
      jobSnapshot,
      setInput,
      runScan,
      runPrepare,
      runOffline,
      runOnline,
      loadScenarioTemplate
    ]
  );

  return <AppStateContext.Provider value={value}>{children}</AppStateContext.Provider>;
}

export function useAppState() {
  const context = useContext(AppStateContext);
  if (!context) {
    throw new Error("useAppState must be used within AppStateProvider");
  }
  return context;
}
