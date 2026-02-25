from typing import List, Dict, Any


class RecordTransformer:
    def __init__(self, config: Dict[str, Any]):
        self.features = config.get("features", {})

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if self.features.get("enable_deduplication", False):
            records = self._deduplicate(records)
            print(f"[Transformer] Deduplication applied.")

        if self.features.get("enable_schema_validation", False):
            records = self._validate_schema(records)
            print(f"[Transformer] Schema validation applied.")

        if self.features.get("enable_audit_trail", False):
            self._write_audit_trail(records)
            print(f"[Transformer] Audit trail written.")

        return records

    def _deduplicate(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        unique = []
        for record in records:
            key = record.get("order_id")
            if key not in seen:
                seen.add(key)
                unique.append(record)
        return unique

    def _validate_schema(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        required_fields = {"order_id", "amount"}
        return [r for r in records if required_fields.issubset(r.keys())]

    def _write_audit_trail(self, records: List[Dict[str, Any]]) -> None:
        print(f"[AuditTrail] Writing {len(records)} records to audit log.")
