# Scenario Execution Results - Tag 55 Mismatch

## Execution Summary

**Status**: ❌ FAILED (as expected)  
**Exit Code**: 2 (Comparison failure)  
**Total Messages**: 3  
**Failed Messages**: 3  
**Matched Comparisons**: 3  

## Detailed Results

### Message 1: New Order (D)
- **ID**: EXIT_QFIX:online:1-1
- **Message Type**: D (New Order Single)
- **Status**: ❌ FAILED
- **Tag 55 Difference**:
  - **Expected**: `MSFT`
  - **Actual**: `AAPL`

### Message 2: Order Cancel/Replace (G)
- **ID**: EXIT_QFIX:online:2-2
- **Message Type**: G (Order Cancel/Replace Request)
- **Status**: ❌ FAILED
- **Tag 55 Difference**:
  - **Expected**: `AMZN`
  - **Actual**: `GOOGL`

### Message 3: Order Cancel (F)
- **ID**: EXIT_QFIX:online:3-3
- **Message Type**: F (Order Cancel Request)
- **Status**: ❌ FAILED
- **Tag 55 Difference**:
  - **Expected**: `NVDA`
  - **Actual**: `TSLA`

## JSON Report (Formatted)

```json
{
  "passed": false,
  "scenario": "scenario-tag55-failure.yml",
  "counts": {
    "sessions": 1,
    "timedOutSessions": 0,
    "sent": 3,
    "received": 3,
    "dropped": 0,
    "matchedComparisons": 3,
    "unmatchedExpected": 0,
    "unmatchedActual": 0,
    "ambiguous": 0
  },
  "sessions": [
    {
      "session": "EXIT_QFIX",
      "sent": 3,
      "received": 3,
      "dropped": 0,
      "timedOut": false,
      "matchedComparisons": 3,
      "unmatchedExpected": 0,
      "unmatchedActual": 0,
      "ambiguous": 0,
      "failureReasons": [
        "3 matched message comparison(s) had tag/value differences"
      ],
      "passed": false
    }
  ],
  "failureSummary": [
    "EXIT_QFIX: 3 matched message comparison(s) had tag/value differences"
  ],
  "diffReport": {
    "totalMessages": 3,
    "failedMessages": 3,
    "messages": [
      {
        "id": "EXIT_QFIX:online:1-1",
        "msgType": "D",
        "passed": false,
        "missingTags": [],
        "extraTags": [],
        "differingValues": {
          "55": {
            "expected": "MSFT",
            "actual": "AAPL"
          }
        }
      },
      {
        "id": "EXIT_QFIX:online:2-2",
        "msgType": "G",
        "passed": false,
        "missingTags": [],
        "extraTags": [],
        "differingValues": {
          "55": {
            "expected": "AMZN",
            "actual": "GOOGL"
          }
        }
      },
      {
        "id": "EXIT_QFIX:online:3-3",
        "msgType": "F",
        "passed": false,
        "missingTags": [],
        "extraTags": [],
        "differingValues": {
          "55": {
            "expected": "NVDA",
            "actual": "TSLA"
          }
        }
      }
    ]
  },
  "linkReports": {
    "EXIT_QFIX": {
      "strategyByMsgType": {
        "D": [11],
        "F": [11],
        "G": [11]
      },
      "counts": {
        "matched": 3,
        "unmatched": 0,
        "ambiguous": 0
      },
      "topCollisions": []
    }
  }
}
```

## Verification

✅ **All 3 messages were successfully matched** (using tag 11 - ClOrdID)  
✅ **All 3 messages showed tag 55 differences** (Symbol field)  
✅ **No messages were dropped or timed out**  
✅ **Comparison engine correctly detected all mismatches**  
✅ **Exit code 2 indicates comparison failure** (not configuration error)  

## Conclusion

The FIX replay harness correctly:
1. Routed all messages through the simulator
2. Matched messages using the linking strategy (tag 11)
3. Detected all tag 55 (Symbol) differences
4. Generated a detailed diff report
5. Failed the scenario with appropriate exit code

This demonstrates that the harness is working as expected and can accurately detect field-level differences in FIX messages.
