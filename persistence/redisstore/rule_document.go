package redisstore

import (
	"encoding/json"
	"fmt"

	"github.com/aditya1808/distributed-rate-limiter/types"
)

// RuleSchemaVersion is the shape of rule records written by this build.
//
// Additive, optional fields do not need a bump. A breaking change bumps this,
// adds a step to upgradeRule, and must be rolled out reader-first: deploy code
// that can read the new version everywhere before any instance writes it.
// Instances that meet a newer record refuse it rather than misread it.
//
// The Lua scripts read "id", "tenant", "namespace" and "version" directly;
// those names are part of every schema version.
const RuleSchemaVersion = 1

type ruleDocument struct {
	SchemaVersion int `json:"schema_version"`
	types.Rule
}

func encodeRule(rule types.Rule) ([]byte, error) {
	return json.Marshal(ruleDocument{SchemaVersion: RuleSchemaVersion, Rule: rule})
}

func decodeRule(data []byte) (types.Rule, error) {
	var head struct {
		SchemaVersion int `json:"schema_version"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return types.Rule{}, err
	}
	if head.SchemaVersion > RuleSchemaVersion {
		return types.Rule{}, fmt.Errorf("%w: record has schema_version %d, this build reads up to %d",
			types.ErrUnsupportedSchema, head.SchemaVersion, RuleSchemaVersion)
	}
	return upgradeRule(head.SchemaVersion, data)
}

// upgradeRule converts a record of any older schema version to the current
// types.Rule. Records written before versioning existed have no field (0) and
// share the v1 layout.
func upgradeRule(version int, data []byte) (types.Rule, error) {
	switch version {
	case 0, 1:
		var doc ruleDocument
		if err := json.Unmarshal(data, &doc); err != nil {
			return types.Rule{}, err
		}
		return doc.Rule, nil
	default:
		return types.Rule{}, fmt.Errorf("%w: no upgrade path from schema_version %d", types.ErrUnsupportedSchema, version)
	}
}
