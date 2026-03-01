export const scenarioTemplate = `inputFolder: ./fixtures/input
expectedFolder: ./fixtures/expected
actualFolder: ./fixtures/actual
reports:
  folder: ./fixtures/reports
  run_online_json: "{scenario}-{timestamp}-run-online-report.json"
  run_online_junit: "{scenario}-{timestamp}-run-online-junit.xml"
  run_offline_json: "{scenario}-{timestamp}-run-offline-report.json"
  run_offline_junit: "{scenario}-{timestamp}-run-offline-junit.xml"
msgTypeFilter: [D, G, F, 8, 3, j]
linker:
  candidateTags: [11, 41, 37, 17, 55, 54, 60]
  candidateCombinationMaxSize: 2
compare:
  defaultExcludeTags: [8, 9, 10, 34, 52, 122, 60]
sessionMappingRules: []
`;
