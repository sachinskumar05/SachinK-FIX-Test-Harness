function toNumberArray(values) {
  if (!Array.isArray(values)) {
    return [];
  }
  return values
    .map(value => Number.parseInt(String(value), 10))
    .filter(value => Number.isFinite(value));
}

function sessionFromId(id) {
  if (typeof id !== "string") {
    return "default";
  }
  const splitAt = id.indexOf(":");
  if (splitAt <= 0) {
    return "default";
  }
  return id.slice(0, splitAt);
}

export function toDiffRows(resultPayload) {
  const messages = resultPayload?.diffReport?.messages;
  if (!Array.isArray(messages)) {
    return [];
  }

  return messages.map(message => {
    const differingValues = message?.differingValues ?? {};
    const differingTags = Object.keys(differingValues)
      .map(tag => Number.parseInt(tag, 10))
      .filter(tag => Number.isFinite(tag));
    const missingTags = toNumberArray(message?.missingTags);
    const extraTags = toNumberArray(message?.extraTags);
    const allTags = [...new Set([...missingTags, ...extraTags, ...differingTags])].sort((a, b) => a - b);

    return {
      id: String(message?.id ?? ""),
      session: sessionFromId(message?.id),
      msgType: message?.msgType ?? "UNKNOWN",
      passed: Boolean(message?.passed),
      missingTags,
      extraTags,
      differingTags,
      differingValues,
      allTags
    };
  });
}

export function toTagDetailRows(diffRow) {
  if (!diffRow) {
    return [];
  }

  const rows = [];

  for (const tag of diffRow.missingTags) {
    rows.push({
      tag,
      expected: "(present in expected)",
      actual: "(missing)"
    });
  }

  for (const tag of diffRow.extraTags) {
    rows.push({
      tag,
      expected: "(not expected)",
      actual: "(present in actual)"
    });
  }

  for (const tag of diffRow.differingTags) {
    const values = diffRow.differingValues?.[String(tag)] ?? {};
    rows.push({
      tag,
      expected: values.expected ?? "(null)",
      actual: values.actual ?? "(null)"
    });
  }

  rows.sort((left, right) => left.tag - right.tag);
  return rows;
}

export function filterDiffRows(rows, { session, msgType, tag }) {
  const normalizedTag = (tag ?? "").trim();
  return rows.filter(row => {
    if (session && session !== "all" && row.session !== session) {
      return false;
    }
    if (msgType && msgType !== "all" && row.msgType !== msgType) {
      return false;
    }
    if (normalizedTag) {
      const match = row.allTags.some(rowTag => String(rowTag).includes(normalizedTag));
      if (!match) {
        return false;
      }
    }
    return true;
  });
}

export function buildSummaryCards(resultPayload, lastOperation) {
  if (!resultPayload) {
    return [];
  }

  if (lastOperation === "scan") {
    return [
      { label: "Files Scanned", value: resultPayload.filesScanned ?? 0 },
      { label: "Messages", value: resultPayload.messageCount ?? 0 },
      { label: "Sessions", value: resultPayload.sessionsDetected?.length ?? 0 },
      { label: "Failures", value: 0 }
    ];
  }

  if (lastOperation === "prepare") {
    const counts = resultPayload.counts ?? {};
    const matched = counts.matched ?? 0;
    const unmatched = counts.unmatched ?? 0;
    const ambiguous = counts.ambiguous ?? 0;
    const total = matched + unmatched + ambiguous;
    return [
      { label: "Matched", value: matched },
      { label: "Unmatched", value: unmatched },
      { label: "Ambiguous", value: ambiguous },
      { label: "Match Rate", value: total > 0 ? `${((matched * 100) / total).toFixed(1)}%` : "n/a" }
    ];
  }

  const counts = resultPayload.counts ?? {};
  const matched = counts.matchedComparisons ?? 0;
  const unmatchedExpected = counts.unmatchedExpected ?? 0;
  const unmatchedActual = counts.unmatchedActual ?? 0;
  const ambiguous = counts.ambiguous ?? 0;
  const failures =
    resultPayload.diffReport?.failedMessages ??
    unmatchedExpected + unmatchedActual + ambiguous;
  const total = matched + failures;

  return [
    { label: "Matched", value: matched },
    { label: "Failures", value: failures },
    { label: "Pass", value: Boolean(resultPayload.passed) ? "Yes" : "No" },
    { label: "Match Rate", value: total > 0 ? `${((matched * 100) / total).toFixed(1)}%` : "n/a" }
  ];
}
