package com.snowflake.labs.kafka.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Compatibility contract for a union of named record branches: an event whose {@code action}
 * field is an Avro union of many <em>named record</em> types rather than of primitives.
 *
 * <p>The rest of the suite exercises unions of primitive branches ({@code ['null','string','int']}).
 * That is a different code path from a union of records, and it is the record-union case that
 * breaks a v3-to-v4 migration: the legacy {@code SnowflakeAvroConverter} collapsed the union to the active
 * branch's fields inlined under {@code action}, whereas Confluent's {@code AvroConverter} emits one
 * key per branch name with all inactive branches null and the active branch nested one level
 * deeper. Every downstream {@code RECORD_CONTENT:action:<field>} path therefore returns NULL after
 * a naive migration.
 *
 * <p>The union is modelled at realistic width: 46 named record branches, three of which carry
 * distinct field lists while the rest carry only a {@code type} discriminator. Exactly one branch
 * is populated per record, so branch count does not change the collapse logic - it only decides
 * whether the logic is exercised at scale.
 */
class NamedRecordUnionCompatibilityTest {

  /** All 46 branch names present under {@code action} in the adopter's v2 schema. */
  private static final List<String> BRANCH_NAMES = List.of(
      "AccountClosed", "AccountCreated", "AccountReopened", "AccountSuspended",
      "AccountVerified", "AddressChanged", "AttachmentAdded", "AttachmentRemoved",
      "ContactUpdated", "CreditCheckResult", "DeviceEnrolled", "DeviceRemoved",
      "DocumentArchived", "DocumentExpired", "DocumentRejected", "DocumentRequested",
      "DocumentSubmitted", "EmailChanged", "EmailVerified", "EntitlementGranted",
      "EntitlementRevoked", "ErrorRaised", "IdentityCheckResult", "LimitChanged",
      "LimitOverridden", "NotificationSent", "OverrideApplied", "PhoneVerified",
      "PlanChanged", "PreferenceUpdated", "ProfileMerged", "ProfileUpdated",
      "RegionMigrated", "ReviewAssigned", "ReviewClosed", "ReviewCompleted",
      "ReviewEscalated", "ReviewOpened", "ReviewPutOnHold", "RiskScoreUpdated",
      "SanctionsCheckResult", "SessionTerminated", "TermsAccepted", "TierChanged",
      "VerificationFailed", "WatchItemDetected");

  /** Branches that carry a distinct field list; all others get a bare {@code type}. */
  private static final Map<String, List<String>> KNOWN_BRANCH_FIELDS = Map.of(
      "AccountCreated", List.of("accountId", "type", "region"),
      "AccountVerified", List.of("accountId", "region", "decision", "decidedVia", "type"),
      "ReviewCompleted", List.of("reason", "result", "type"));

  /** Sanitized values keyed by field name. */
  private static final Map<String, String> SANITIZED = Map.of(
      "accountId", "ACCT0000000001",
      "region", "region-1",
      "decision", "Pass",
      "decidedVia", "Automated",
      "reason", "policyOverride",
      "result", "Pass");

  private static final String EVENT_ID = "00000000-0000-4000-8000-000000000001";
  private static final String TIMESTAMP = "2026-06-25T15:05:17.671Z";

  private AvroTestSupport support;

  @BeforeEach
  void setUp() {
    support = new AvroTestSupport();
  }

  // ---------------------------------------------------------------------------------------------
  // Schema construction
  // ---------------------------------------------------------------------------------------------

  /** Builds one branch record schema: known field list, or a bare {@code type} discriminator. */
  private static Schema branchSchema(String branchName) {
    SchemaBuilder.FieldAssembler<Schema> fields =
        SchemaBuilder.record(branchName).namespace("com.example.events").fields();
    for (String field : KNOWN_BRANCH_FIELDS.getOrDefault(branchName, List.of("type"))) {
      fields = fields.requiredString(field);
    }
    return fields.endRecord();
  }

  /**
   * Builds the event schema. {@code action} is a union of all 46 branch records, optionally
   * prefixed with a {@code null} branch.
   */
  private static Schema eventSchema(boolean nullableAction, List<String> branchNames) {
    List<Schema> branches = new ArrayList<>();
    if (nullableAction) {
      branches.add(Schema.create(Schema.Type.NULL));
    }
    for (String name : branchNames) {
      branches.add(branchSchema(name));
    }
    Schema actionUnion = Schema.createUnion(branches);

    Schema stringArray = Schema.createArray(Schema.create(Schema.Type.STRING));
    List<Schema.Field> fields = List.of(
        new Schema.Field("eventId", Schema.create(Schema.Type.STRING), null, (Object) null),
        new Schema.Field("action", actionUnion, null,
            nullableAction ? Schema.Field.NULL_DEFAULT_VALUE : null),
        new Schema.Field("relatedIds", stringArray, null, (Object) null),
        new Schema.Field("attributesByProvider", stringArray, null, (Object) null),
        new Schema.Field("timestamp", Schema.create(Schema.Type.STRING), null, (Object) null));

    Schema record = Schema.createRecord("DomainEvent", null, "com.example.events", false);
    record.setFields(fields);
    return record;
  }

  /** Populates an event with exactly one active branch, or none when {@code branchName} is null. */
  private static GenericRecord event(Schema eventSchema, String branchName) {
    GenericRecord event = new GenericData.Record(eventSchema);
    event.put("eventId", EVENT_ID);
    event.put("timestamp", TIMESTAMP);
    event.put("relatedIds", new ArrayList<>());
    event.put("attributesByProvider", new ArrayList<>());

    if (branchName == null) {
      event.put("action", null);
      return event;
    }

    Schema branch = eventSchema.getField("action").schema().getTypes().stream()
        .filter(s -> s.getType() == Schema.Type.RECORD && s.getName().equals(branchName))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("no such branch: " + branchName));

    GenericRecord action = new GenericData.Record(branch);
    for (Schema.Field field : branch.getFields()) {
      action.put(field.name(),
          "type".equals(field.name()) ? branchName : SANITIZED.get(field.name()));
    }
    event.put("action", action);
    return event;
  }

  /** Expected legacy {@code action} object: the active branch's fields, inlined. */
  private static Map<String, String> expectedLegacyAction(String branchName) {
    Map<String, String> expected = new LinkedHashMap<>();
    for (String field : KNOWN_BRANCH_FIELDS.get(branchName)) {
      expected.put(field, "type".equals(field) ? branchName : SANITIZED.get(field));
    }
    return expected;
  }

  // ---------------------------------------------------------------------------------------------
  // The compatibility contract
  // ---------------------------------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(strings = {"AccountCreated", "AccountVerified", "ReviewCompleted"})
  @DisplayName("compat converter collapses the record union to the legacy inlined shape")
  void compatConverterInlinesActiveBranchUnderAction(String branchName) throws Exception {
    Schema schema = eventSchema(false, BRANCH_NAMES);
    AvroTestSupport.ThreePathComparison result =
        support.compareAll(schema, event(schema, branchName));

    JsonNode action = result.compat.at("/action");
    Map<String, String> expected = expectedLegacyAction(branchName);

    assertEquals(expected.size(), action.size(),
        "action must contain only the active branch's fields, got: " + action);
    for (Map.Entry<String, String> entry : expected.entrySet()) {
      assertEquals(entry.getValue(), action.at("/" + entry.getKey()).textValue(),
          "action:" + entry.getKey() + " must survive the compat path");
    }
    assertTrue(action.at("/" + branchName).isMissingNode(),
        "the branch name must not appear as a wrapper key under action");
  }

  @ParameterizedTest
  @ValueSource(strings = {"AccountCreated", "AccountVerified", "ReviewCompleted"})
  @DisplayName("compat converter output matches the v3 legacy oracle exactly")
  void compatConverterAgreesWithLegacyOracle(String branchName) throws Exception {
    Schema schema = eventSchema(false, BRANCH_NAMES);
    AvroTestSupport.ThreePathComparison result =
        support.compareAll(schema, event(schema, branchName));

    assertEquals(result.legacy.at("/action"), result.compat.at("/action"),
        "compat action must equal the legacy oracle for branch " + branchName);
  }

  @ParameterizedTest
  @ValueSource(strings = {"AccountCreated", "AccountVerified", "ReviewCompleted"})
  @DisplayName("regression pin: the standard Confluent path breaks legacy action paths")
  void confluentPathExpandsUnionAndBreaksLegacyPaths(String branchName) throws Exception {
    Schema schema = eventSchema(false, BRANCH_NAMES);
    AvroTestSupport.ThreePathComparison result =
        support.compareAll(schema, event(schema, branchName));

    JsonNode action = result.current.at("/action");
    assertEquals(BRANCH_NAMES.size(), action.size(),
        "current path is expected to expand every union branch");
    assertTrue(action.at("/type").isMissingNode(),
        "RECORD_CONTENT:action:type is expected to be absent on the current path");
    assertFalse(action.at("/" + branchName).isMissingNode(),
        "current path nests the active branch under its branch name");

    // The inactive branches are present and null - this is the reported symptom.
    long nulls = 0;
    for (JsonNode value : action) {
      if (value.isNull()) {
        nulls++;
      }
    }
    assertEquals(BRANCH_NAMES.size() - 1, nulls, "all inactive branches should be null");
  }

  @ParameterizedTest
  @ValueSource(strings = {"AccountCreated", "AccountVerified", "ReviewCompleted"})
  @DisplayName("sibling top-level fields survive the collapse")
  void topLevelSiblingsArePreserved(String branchName) throws Exception {
    Schema schema = eventSchema(false, BRANCH_NAMES);
    AvroTestSupport.ThreePathComparison result =
        support.compareAll(schema, event(schema, branchName));

    assertEquals(EVENT_ID, result.compat.at("/eventId").textValue());
    assertEquals(TIMESTAMP, result.compat.at("/timestamp").textValue());
    assertTrue(result.compat.at("/relatedIds").isArray(),
        "relatedIds must remain an array");
    assertTrue(result.compat.at("/attributesByProvider").isArray(),
        "attributesByProvider must remain an array");
    assertEquals(5, result.compat.size(), "no top-level field may be added or dropped");
  }

  @Test
  @DisplayName("a nullable action union with no active branch yields null, and the record survives")
  void nullActionIsPreservedAndRecordIsRetained() throws Exception {
    Schema schema = eventSchema(true, BRANCH_NAMES);
    AvroTestSupport.ThreePathComparison result = support.compareAll(schema, event(schema, null));

    assertTrue(result.compat.at("/action").isNull(),
        "an entirely absent action must be null, not an empty object or a missing key");
    assertEquals(EVENT_ID, result.compat.at("/eventId").textValue(),
        "the record must be retained when no branch is active");
    assertEquals(result.legacy.at("/action"), result.compat.at("/action"),
        "null action must match the legacy oracle");
  }

  @Test
  @DisplayName("adding a 47th union branch needs no plugin rebuild")
  void newUnionBranchIsHandledByTheSameConverterInstance() throws Exception {
    // v1 of the schema: the 46 known branches.
    Schema v1 = eventSchema(false, BRANCH_NAMES);
    byte[] v1Payload = support.serialize(v1, event(v1, "AccountVerified"));

    // v2 adds a branch, as happens when a producer ships a new event type.
    List<String> evolved = new ArrayList<>(BRANCH_NAMES);
    evolved.add("ZzNewBranchType");
    Schema v2 = eventSchema(false, evolved);
    byte[] v2Payload = support.serialize(v2, event(v2, "ZzNewBranchType"));

    // One converter instance must handle both, resolving each payload's writer schema by ID.
    org.apache.kafka.connect.storage.Converter converter =
        support.tryCreateCompatConverter().orElseThrow();

    JsonNode fromV1 = support.recordContent(converter, v1Payload);
    assertEquals("AccountVerified", fromV1.at("/action/type").textValue(),
        "pre-evolution payloads must still collapse correctly");

    JsonNode fromV2 = support.recordContent(converter, v2Payload);
    assertEquals("ZzNewBranchType", fromV2.at("/action/type").textValue(),
        "a newly added branch must collapse without redeploying the plugin");
    assertEquals(1, fromV2.at("/action").size(),
        "the new branch must be inlined, not wrapped");
  }
}
