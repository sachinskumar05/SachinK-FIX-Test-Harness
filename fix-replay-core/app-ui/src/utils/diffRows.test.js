import { describe, expect, it } from "vitest";
import { filterDiffRows, toDiffRows, toTagDetailRows } from "./diffRows";

describe("diffRows", () => {
  it("builds diff rows and extracts session + tags", () => {
    const rows = toDiffRows({
      diffReport: {
        messages: [
          {
            id: "BUY_SELL:message-1",
            msgType: "8",
            passed: false,
            missingTags: [11],
            extraTags: [60],
            differingValues: {
              55: { expected: "MSFT", actual: "AAPL" }
            }
          }
        ]
      }
    });

    expect(rows).toHaveLength(1);
    expect(rows[0].session).toBe("BUY_SELL");
    expect(rows[0].allTags).toEqual([11, 55, 60]);
  });

  it("supports session/msgType/tag filtering", () => {
    const rows = toDiffRows({
      diffReport: {
        messages: [
          { id: "A_B:1", msgType: "D", passed: false, missingTags: [11], extraTags: [], differingValues: {} },
          { id: "C_D:2", msgType: "8", passed: false, missingTags: [37], extraTags: [], differingValues: {} }
        ]
      }
    });

    const filtered = filterDiffRows(rows, { session: "A_B", msgType: "D", tag: "11" });
    expect(filtered).toHaveLength(1);
    expect(filtered[0].id).toBe("A_B:1");
  });

  it("builds side-by-side details", () => {
    const [row] = toDiffRows({
      diffReport: {
        messages: [
          {
            id: "message-3",
            msgType: "D",
            passed: false,
            missingTags: [41],
            extraTags: [],
            differingValues: { 55: { expected: "MSFT", actual: "AAPL" } }
          }
        ]
      }
    });

    const detail = toTagDetailRows(row);
    expect(detail.map(item => item.tag)).toEqual([41, 55]);
    expect(detail[0].actual).toBe("(missing)");
  });
});
