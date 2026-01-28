# FEATURE_1: Case-Insensitive Column Matching

## Overview
Implement case-insensitive matching for Notion property names throughout the application to provide robustness against property name case variations in Notion databases.

## Problem Statement
The current implementation uses exact case-sensitive matching when accessing Notion property names from webhook payloads. This creates fragility where:
- Webhooks with `Date` work but `date` fails
- Webhooks with `Cronograma` work but `cronograma` fails
- Property name mismatches cause silent failures or error responses
- Users have no control over Notion's property name casing

## Current Behavior Analysis

### Webhook Handler
The webhook endpoint in `api/webhooks.py` uses exact string matching:
- `properties.get("Date")` - Only matches uppercase "D"
- `properties.get("Departure") or properties.get("departure")` - Has partial fallback

### Notion Client
The Notion API client in `clients/notion.py` uses hardcoded property names:
- Query filters: `{"property": "Día", ...}`
- Update properties: `{"Cronograma": {"relation": [...]}}`

## Proposed Solution Architecture

### Core Utility Layer
Create a new utility module `core/utils.py` that provides:
- Case-insensitive property lookup function
- Property existence checker
- Helper to normalize property names to canonical forms

The utility will:
- Iterate through property dictionary keys
- Compare normalized (lowercased) versions
- Return the original value from the matching key
- Log the actual key matched for debugging

### Property Name Constants
Define canonical property names in a dedicated constants class within the Notion client:
- Separates business logic from hardcoded strings
- Centralizes property name definitions
- Documents expected Notion schema
- Makes future property changes easier

### Integration Points

#### Webhook Layer
Update webhook payload parsing to:
- Use case-insensitive lookup for all property extraction
- Log warnings when matched with non-canonical casing
- Maintain backward compatibility with existing tests

#### Notion Client Layer
Update Notion client methods to:
- Use property name constants for API calls
- Keep API calls case-sensitive (Notion API requirement)
- Add documentation about case-sensitive API vs case-insensitive payload handling
- Note: Only webhook payloads use case-insensitive lookup; all Notion API calls remain case-sensitive

## Implementation Steps

### Phase 1: Core Utilities
1. Create `core/utils.py` module
2. Implement case-insensitive property lookup function
3. Implement property existence checker function
4. Add comprehensive docstrings with examples

### Phase 2: Webhook Handler Updates
1. Import new utility functions
2. Replace all `properties.get()` calls with case-insensitive lookup
3. Add debug logging for matched property names
4. Test with existing test suite

### Phase 3: Notion Client Updates
1. Define PropertyNames constant class
2. Update all hardcoded property references
3. Ensure API calls use exact property names from constants
4. Add inline documentation

### Phase 4: Testing
1. Create unit tests for utility functions
2. Add integration tests for webhook case handling
3. Test all case variations: lowercase, uppercase, mixed case
4. Verify backward compatibility

## Testing Strategy

### Unit Tests
Test utility function edge cases:
- Exact case match
- All lowercase search
- All uppercase search
- Mixed case search
- Non-existent properties
- Empty dictionaries
- Properties with similar names (Date vs Date2)

### Integration Tests
Test webhook endpoint with:
- `date`, `DATE`, `Date`, `dAtE`
- `departure`, `DEPARTURE`, `Departure`
- Multiple properties with different casings

### Regression Tests
Ensure all existing tests pass without modification:
- Cronograma sync workflow tests
- Pasajes sync workflow tests
- Webhook endpoint tests
- Authentication tests

## Branch Strategy
Branch name: `feature/case-insensitive-matching`

Commit sequence:
1. Add core utility module with lookup functions
2. Update webhook handler to use case-insensitive lookup
3. Update Notion client with property name constants
4. Add unit tests for utility functions
5. Add integration tests for webhook case handling
6. Run linting and formatting

## Success Criteria
- All existing tests pass without modification
- New unit tests cover 100% of utility functions
- Integration tests verify all case variations work
- Code passes linting and formatting checks
- Test coverage remains high
- No performance degradation

## Migration Notes
- No breaking changes to external APIs
- Backward compatible with existing webhooks
- Users with correct case continue to work
- Users with incorrect case now work (new capability)
- No database changes required

## Risks and Mitigations
- **Risk**: Performance impact from string normalization
  - **Mitigation**: Lowercase operation is fast, only runs on webhook events
- **Risk**: Matches unintended similar property names
  - **Mitigation**: Exact lowercase match prevents partial matches
- **Risk**: Confusion about API vs payload case sensitivity
  - **Mitigation**: Clear documentation and code comments
