# Scenario: Tag 55 (Symbol) Mismatch - Failure Test

## Purpose

This scenario is designed to **intentionally fail** to demonstrate the FIX replay harness's ability to detect differences in FIX tag 55 (Symbol field).

## Test Design

### Input Messages (ENTRY_QFIX.in)
Three FIX messages with the following symbols:
1. **Message 1 (New Order - D)**: Symbol = `AAPL`
2. **Message 2 (Order Cancel/Replace - G)**: Symbol = `GOOGL`
3. **Message 3 (Order Cancel - F)**: Symbol = `TSLA`

### Expected Messages (EXIT_QFIX.out)
Three FIX messages with **different** symbols:
1. **Message 1 (New Order - D)**: Symbol = `MSFT`
2. **Message 2 (Order Cancel/Replace - G)**: Symbol = `AMZN`
3. **Message 3 (Order Cancel - F)**: Symbol = `NVDA`

## Expected Behavior

When this scenario is executed, the FIX replay harness should:

1. ✅ Successfully route all 3 messages through the simulator
2. ✅ Compare received messages against expected messages
3. ❌ **FAIL** the comparison for all 3 messages due to tag 55 mismatch
4. ✅ Generate a detailed diff report showing:
   - Tag 55 expected value (MSFT, AMZN, NVDA)
   - Tag 55 actual value (AAPL, GOOGL, TSLA)
   - Clear indication of the mismatch

## Running the Scenario

### Online Mode (with simulator):
```bash
cd fix-replay-core
.\gradlew.bat :app-cli:run --args="run-online --scenario test-suites/scenario-tag55-mismatch/scenario-tag55-failure.yml --start-simulator"
```

### Offline Mode (comparison only):
```bash
.\gradlew.bat :app-cli:run --args="run-offline --scenario test-suites/scenario-tag55-mismatch/scenario-tag55-failure.yml"
```

## Expected Results

### Exit Code
- **Exit Code 2** - Comparison failure (not a configuration error)

### Diff Report
The diff report should show 3 failed message comparisons:

```json
{
  "passed": false,
  "diffReport": {
    "messages": [
      {
        "id": "EXIT:1",
        "result": {
          "msgType": "D",
          "passed": false,
          "tagDifferences": {
            "55": {
              "expected": "MSFT",
              "actual": "AAPL"
            }
          }
        }
      },
      {
        "id": "EXIT:2",
        "result": {
          "msgType": "G",
          "passed": false,
          "tagDifferences": {
            "55": {
              "expected": "AMZN",
              "actual": "GOOGL"
            }
          }
        }
      },
      {
        "id": "EXIT:3",
        "result": {
          "msgType": "F",
          "passed": false,
          "tagDifferences": {
            "55": {
              "expected": "NVDA",
              "actual": "TSLA"
            }
          }
        }
      }
    ]
  }
}
```

### JUnit XML
The JUnit XML report should show 3 test failures with clear error messages indicating the tag 55 mismatch.

## FIX Tag 55 Reference

**Tag 55 - Symbol**
- **Type**: String
- **Description**: Ticker symbol (e.g., AAPL, MSFT, GOOGL)
- **Required**: Yes (for most order-related messages)
- **Category**: Instrument Identification

## Use Cases

This failure scenario is useful for:
1. **Validation Testing** - Ensuring the comparison engine correctly detects symbol mismatches
2. **CI/CD Testing** - Verifying that the harness properly fails when differences are detected
3. **Regression Testing** - Confirming that tag 55 is not accidentally excluded from comparison
4. **Demo/Training** - Showing how the diff report highlights specific field differences

## Notes

- Tag 55 is **NOT** in the `defaultExcludeTags` list, so it will be compared
- All other message fields (except excluded tags) should match
- The simulator will pass through the symbols unchanged (mutation disabled)
- This scenario demonstrates the harness working correctly by **failing as expected**
