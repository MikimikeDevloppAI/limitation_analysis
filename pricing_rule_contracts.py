"""
Pricing rule data models for BAG limitation cashback formalization.

Adapted from VictorHueni/swiss-aos-drug-reimbursement-model contracts.
Formalizes cashback rules as structured decision tables with condition ASTs,
refund outcomes, and grounding evidence.

Used by step_05_pricing_rules.py to transform detected cashback (step_04)
into machine-readable pricing rules stored as JSON in the database.
"""

from __future__ import annotations

import json
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


# ============================================================
# Enums
# ============================================================

class RuleFamily(StrEnum):
    """High-level classification of the pricing rule."""
    PRICE = "PRICE"           # Standard price-based refund (%, CHF, per unit)
    VOLUME_CAP = "VOLUME_CAP" # Refund triggered by volume threshold
    P4P = "P4P"               # Pay-for-performance (refund if treatment fails)
    UNKNOWN = "UNKNOWN"


class TransparencyVariant(StrEnum):
    PUBLIC = "PUBLIC"
    CONFIDENTIAL = "CONFIDENTIAL"
    UNKNOWN = "UNKNOWN"


class RefundType(StrEnum):
    FIXED_AMOUNT = "FIXED_AMOUNT"
    PERCENTAGE = "PERCENTAGE"
    TOTAL_COST = "TOTAL_COST"
    UNKNOWN = "UNKNOWN"


class PriceReferenceBasis(StrEnum):
    PUBLIC = "PUBLIC"          # Prix public (PP)
    EX_FACTORY = "EX_FACTORY"  # Prix de fabrique (PF/PEX)
    UNKNOWN = "UNKNOWN"


class QuantityUnit(StrEnum):
    PER_PACK = "PER_PACK"
    PER_FLACON = "PER_FLACON"
    PER_INJECTION = "PER_INJECTION"
    PER_MG = "PER_MG"
    PER_PATIENT = "PER_PATIENT"
    PER_CYCLE = "PER_CYCLE"
    PER_TREATMENT = "PER_TREATMENT"
    PER_MONTH = "PER_MONTH"
    PER_WEEK = "PER_WEEK"
    PER_YEAR = "PER_YEAR"
    PER_ADMINISTRATION = "PER_ADMINISTRATION"
    UNKNOWN = "UNKNOWN"


class RefundScope(StrEnum):
    FULL_PACK = "FULL_PACK"
    PARTIAL = "PARTIAL"
    FIRST_N_ONLY = "FIRST_N_ONLY"
    FROM_NTH_ONWARD = "FROM_NTH_ONWARD"
    ALL_ELIGIBLE = "ALL_ELIGIBLE"
    UNKNOWN = "UNKNOWN"


class ConditionOperator(StrEnum):
    EQ = "EQ"
    NE = "NE"
    GT = "GT"
    GTE = "GTE"
    LT = "LT"
    LTE = "LTE"
    BETWEEN = "BETWEEN"
    IN = "IN"
    NOT_IN = "NOT_IN"
    EXISTS = "EXISTS"
    NOT_EXISTS = "NOT_EXISTS"
    IS_TRUE = "IS_TRUE"
    IS_FALSE = "IS_FALSE"


class TherapyContext(StrEnum):
    """Whether the drug is used alone or in combination."""
    MONOTHERAPY = "MONOTHERAPY"
    COMBINATION = "COMBINATION"
    UNKNOWN = "UNKNOWN"


class RuleParameter(StrEnum):
    """Fields that can appear in condition predicates."""
    PACK_COUNT = "PACK_COUNT"
    ADMINISTRATION_EVENT_COUNT = "ADMINISTRATION_EVENT_COUNT"
    CYCLE_COUNT = "CYCLE_COUNT"
    THERAPY_DURATION = "THERAPY_DURATION"
    PATIENT_COUNT = "PATIENT_COUNT"
    DOSE_MG = "DOSE_MG"
    LINE_OF_THERAPY = "LINE_OF_THERAPY"
    PROGRESSION_FLAG = "PROGRESSION_FLAG"
    RELAPSE_FLAG = "RELAPSE_FLAG"
    INDICATION_CODE = "INDICATION_CODE"
    PRICE_REFERENCE_BASIS = "PRICE_REFERENCE_BASIS"
    ADMINISTRATION_START_FLAG = "ADMINISTRATION_START_FLAG"
    FIRST_INSURER_REQUEST_FLAG = "FIRST_INSURER_REQUEST_FLAG"
    THERAPY_CONTEXT = "THERAPY_CONTEXT"
    COMBINATION_PARTNERS = "COMBINATION_PARTNERS"
    UNKNOWN = "UNKNOWN"


class EvidenceType(StrEnum):
    EXPLICIT_TEXT = "EXPLICIT_TEXT"
    INFERRED = "INFERRED"


# ============================================================
# Models
# ============================================================

class RuleBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class GroundingEvidence(RuleBaseModel):
    """Verbatim quote from the source text supporting a field extraction."""
    field_name: str
    source_quote: str
    evidence_type: EvidenceType


class RefundOutcome(RuleBaseModel):
    """What the manufacturer refunds."""
    refund_type: RefundType
    price_reference_basis: PriceReferenceBasis = PriceReferenceBasis.UNKNOWN
    quantity_unit: QuantityUnit = QuantityUnit.UNKNOWN
    refund_scope: RefundScope = RefundScope.UNKNOWN
    value: float | None = None
    currency: str | None = None
    is_confidential: bool = False
    reference_price: float | None = None


class InputVariable(RuleBaseModel):
    """An input required to evaluate the decision table."""
    name: RuleParameter
    data_type: Literal["int", "float", "str", "bool", "date"]
    description: str


class ConditionPredicate(RuleBaseModel):
    """A single condition: field op value."""
    kind: Literal["PREDICATE"] = "PREDICATE"
    field: RuleParameter
    op: ConditionOperator
    value: Any | None = None
    unit: QuantityUnit | None = None


class ConditionGroup(RuleBaseModel):
    """Logical grouping of conditions (ALL = AND, ANY = OR)."""
    kind: Literal["GROUP"] = "GROUP"
    logic: Literal["ALL", "ANY"]
    clauses: list[ConditionNode] = Field(min_length=1)


ConditionNode = Annotated[
    ConditionGroup | ConditionPredicate,
    Field(discriminator="kind"),
]


class DecisionRow(RuleBaseModel):
    """One row in the decision table: condition -> outcome."""
    sequence: int = Field(ge=1)
    condition_ast: ConditionNode
    outcome: RefundOutcome
    explanation: str | None = None
    critical_field_evidence: list[GroundingEvidence] = Field(default_factory=list)


class IndicationContext(RuleBaseModel):
    """Optional indication context for the rule."""
    indication_code: str | None = None
    indication_name: str | None = None
    therapy_context: TherapyContext = TherapyContext.UNKNOWN
    combination_partners: list[str] = Field(default_factory=list)


class PricingRuleStructure(RuleBaseModel):
    """
    Top-level pricing rule model.

    Represents a formalized cashback/pricing agreement extracted from
    a BAG limitation text. Contains:
    - rule_id: unique identifier (text_id based)
    - model_family: PRICE / VOLUME_CAP / P4P / UNKNOWN
    - decision_table: condition AST -> refund outcome rows
    - required_inputs: variables needed to evaluate conditions
    - evidence: grounded quotes from source text
    """
    rule_id: str
    rule_summary: str | None = None
    model_family: RuleFamily
    transparency_variant: TransparencyVariant = TransparencyVariant.UNKNOWN
    context: IndicationContext | None = None
    required_inputs: list[InputVariable] = Field(default_factory=list)
    decision_table: list[DecisionRow] = Field(default_factory=list)
    default_outcome: RefundOutcome | None = None

    def to_json(self) -> str:
        return self.model_dump_json(exclude_none=True, indent=2)

    def to_dict(self) -> dict:
        return self.model_dump(exclude_none=True)


# Rebuild for forward references
ConditionGroup.model_rebuild()


# ============================================================
# Unit mapping from cashback_extractor output
# ============================================================

UNIT_MAP = {
    "per_box": QuantityUnit.PER_PACK,
    "per_pack": QuantityUnit.PER_PACK,
    "per_flacon": QuantityUnit.PER_FLACON,
    "per_syringe": QuantityUnit.PER_INJECTION,
    "per_pen": QuantityUnit.PER_INJECTION,
    "per_dose": QuantityUnit.PER_INJECTION,
    "per_injection": QuantityUnit.PER_INJECTION,
    "per_mg": QuantityUnit.PER_MG,
    "per_patient": QuantityUnit.PER_PATIENT,
    "per_cycle": QuantityUnit.PER_CYCLE,
    "per_treatment": QuantityUnit.PER_TREATMENT,
    "per_month": QuantityUnit.PER_MONTH,
    "per_week": QuantityUnit.PER_WEEK,
    "per_year": QuantityUnit.PER_YEAR,
}

CALC_TYPE_MAP = {
    "percentage": RefundType.PERCENTAGE,
    "percentage_implicit": RefundType.PERCENTAGE,
    "chf_fixed": RefundType.FIXED_AMOUNT,
    "per_mg": RefundType.FIXED_AMOUNT,
    "per_mg_centimes": RefundType.FIXED_AMOUNT,
    "full_refund": RefundType.TOTAL_COST,
    "full_refund_pex": RefundType.TOTAL_COST,
    "amount_number_only": RefundType.FIXED_AMOUNT,
    "cost_refund": RefundType.TOTAL_COST,
    "undisclosed": RefundType.UNKNOWN,
    "undisclosed_fixed": RefundType.UNKNOWN,
    "amount_unspecified": RefundType.UNKNOWN,
    "threshold_box": RefundType.UNKNOWN,
}

CALC_BASIS_MAP = {
    "full_refund_pex": PriceReferenceBasis.EX_FACTORY,
}
