"""
Pydantic V2 models for SEC EDGAR API responses.

Bronze layer — raw ingest. ALL fields from the API are captured.
No filtering, no concept selection, no period-type exclusions.

References:
  https://www.sec.gov/developer
  https://data.sec.gov/submissions/CIK{cik10}.json
  https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field, model_validator


# ── Submissions ───────────────────────────────────────────────────────────────

class Address(BaseModel):
    model_config = ConfigDict(extra="allow")

    street1:                    Optional[str] = None
    street2:                    Optional[str] = None
    city:                       Optional[str] = None
    stateOrCountry:             Optional[str] = None
    zipCode:                    Optional[str] = None
    stateOrCountryDescription:  Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _drop_country_code(cls, data: object) -> object:
        if isinstance(data, dict):
            data = dict(data)
            data.pop("countryCode", None)
        return data


class FormerName(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    # SEC submissions currently return formerNames entries with "name", "from", and "to".
    # Keep all of them nullable so sparse historical records still validate.
    name: Optional[str] = None
    from_: Optional[str] = Field(default=None, alias="from")
    to: Optional[str] = None
    date: Optional[str] = None


class Filing(BaseModel):
    model_config = ConfigDict(extra="allow")

    """One row from the filings.recent columnar arrays."""
    accessionNumber:        str
    filingDate:             str
    reportDate:             Optional[str] = None
    acceptanceDateTime:     Optional[str] = None
    act:                    Optional[str] = None
    form:                   str
    fileNumber:             Optional[str] = None
    filmNumber:             Optional[str] = None
    items:                  Optional[str] = None
    core_type:              Optional[str] = None
    size:                   Optional[int] = None
    isXBRL:                 Optional[int] = None
    isXBRLNumeric:          Optional[int] = None
    isInlineXBRL:           Optional[int] = None
    primaryDocument:        Optional[str] = None
    primaryDocDescription:  Optional[str] = None


class FilingsFile(BaseModel):
    model_config = ConfigDict(extra="allow")
    """Entry in filings.files — pointer to an older submissions batch."""
    name:         str
    filingCount:  int
    filingFrom:   Optional[str] = None
    filingTo:     Optional[str] = None


class RecentFilings(BaseModel):
    model_config = ConfigDict(extra="allow")

    accessionNumber:        list[str] = Field(default_factory=list)
    filingDate:             list[str] = Field(default_factory=list)
    reportDate:             list[Optional[str]] = Field(default_factory=list)
    acceptanceDateTime:     list[Optional[str]] = Field(default_factory=list)
    act:                    list[Optional[str]] = Field(default_factory=list)
    form:                   list[str] = Field(default_factory=list)
    fileNumber:             list[Optional[str]] = Field(default_factory=list)
    filmNumber:             list[Optional[str]] = Field(default_factory=list)
    items:                  list[Optional[str]] = Field(default_factory=list)
    core_type:              list[Optional[str]] = Field(default_factory=list)
    size:                   list[Optional[int]] = Field(default_factory=list)
    isXBRL:                 list[Optional[int]] = Field(default_factory=list)
    isXBRLNumeric:          list[Optional[int]] = Field(default_factory=list)
    isInlineXBRL:           list[Optional[int]] = Field(default_factory=list)
    primaryDocument:        list[Optional[str]] = Field(default_factory=list)
    primaryDocDescription:  list[Optional[str]] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_parallel_array_lengths(self) -> RecentFilings:
        lengths = {field: len(value) for field, value in self.model_dump().items() if value}
        if lengths and len(set(lengths.values())) != 1:
            raise ValueError(f"filings.recent arrays must have matching lengths, got {lengths}")
        return self


class Filings(BaseModel):
    model_config = ConfigDict(extra="allow")

    recent: Optional[RecentFilings] = None
    files: list[FilingsFile] = Field(default_factory=list)


class Submission(BaseModel):
    model_config = ConfigDict(extra="allow")

    """
    Full company submission record from SEC EDGAR.
    Maps every top-level field returned by the submissions endpoint.
    filings.recent is NOT stored here — use explode_filings() to get Filing rows.
    """
    cik:                                str
    entityType:                         Optional[str] = None
    sic:                                Optional[str] = None
    sicDescription:                     Optional[str] = None
    ownerOrg:                           Optional[str] = None
    insiderTransactionForOwnerExists:   Optional[int] = None
    insiderTransactionForIssuerExists:  Optional[int] = None
    name:                               str
    tickers:                            list[str] = Field(default_factory=list)
    exchanges:                          list[str] = Field(default_factory=list)
    ein:                                Optional[str] = None
    lei:                                Optional[str] = None
    description:                        Optional[str] = None
    website:                            Optional[str] = None
    investorWebsite:                    Optional[str] = None
    category:                           Optional[str] = None
    fiscalYearEnd:                      Optional[str] = None
    stateOfIncorporation:               Optional[str] = None
    stateOfIncorporationDescription:    Optional[str] = None
    phone:                              Optional[str] = None
    flags:                              Optional[str] = None
    formerNames:                        list[FormerName] = Field(default_factory=list)
    addresses:                          Optional[dict[str, Address]] = None
    filings_recent:                     Optional[RecentFilings] = Field(default=None, exclude=True)
    filing_files:                       list[FilingsFile] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _extract_filing_data(cls, data: dict) -> dict:
        """Validate nested filings payload, then lift the pieces we keep on the model."""
        data = dict(data)
        filings_raw = data.pop("filings", None) or {}
        filings = Filings.model_validate(filings_raw)
        data["filings_recent"] = filings.recent
        data["filing_files"] = filings.files
        return data


# ── CompanyFacts ──────────────────────────────────────────────────────────────

class FactEntry(BaseModel):
    model_config = ConfigDict(extra="allow")
    """
    One tagged value from an XBRL filing.
    start is absent for instant facts (balance sheet items at a point in time).
    frame is present when SEC has mapped the entry to a standard calendar period.
    """
    accn:   str
    end:    str
    start:  Optional[str] = None   # absent for instant facts
    val:    Decimal
    form:   str
    filed:  str
    frame:  Optional[str] = None   # e.g. "CY2023", "CY2023Q3I", "CY2023Q3"


class ConceptData(BaseModel):
    model_config = ConfigDict(extra="allow")
    """All reported values for one XBRL concept (e.g. us-gaap/Revenues)."""
    label:        Optional[str] = None
    description:  Optional[str] = None
    units:        dict[str, list[FactEntry]]   # unit → entries, e.g. "USD" → [...]


class CompanyFacts(BaseModel):
    model_config = ConfigDict(extra="allow")
    """
    Full XBRL company facts response.
    facts is a nested dict: namespace → concept_name → ConceptData.
    Common namespaces: "us-gaap", "dei", "ifrs-full".
    """
    cik:         int
    entityName:  str
    facts:       dict[str, dict[str, ConceptData]]


# ── Explosion helpers ─────────────────────────────────────────────────────────

def explode_filings(cik10: str, recent: dict) -> list[dict]:
    """
    Convert filings.recent columnar arrays to a list of Filing dicts.

    The SEC API stores recent filings as parallel arrays (one key per field,
    each value is a list where index i corresponds to filing i). This function
    transposes that into row-oriented dicts and validates each via Filing model.

    Returns plain dicts (not Pydantic objects) for easy DataFrame construction.
    """
    if not recent:
        return []
    keys = list(recent.keys())
    rows = []
    for vals in zip(*recent.values()):
        row = dict(zip(keys, vals))
        filing = Filing.model_validate(row)
        d = filing.model_dump()
        d["cik"] = cik10
        rows.append(d)
    return rows


def explode_facts(parsed: CompanyFacts) -> list[dict]:
    """
    Flatten all XBRL facts to a list of dicts — every namespace, every concept,
    every unit, every entry. No filtering whatsoever.

    Each row represents one tagged value from one filing:
      cik, entity_name, namespace, concept, label, unit,
      end, start, val, accn, form, filed, frame
    """
    rows = []
    cik10 = str(parsed.cik).zfill(10)
    for namespace, concepts in parsed.facts.items():
        for concept_name, concept_data in concepts.items():
            for unit, entries in concept_data.units.items():
                for entry in entries:
                    rows.append({
                        "cik":          cik10,
                        "entity_name":  parsed.entityName,
                        "namespace":    namespace,       # "us-gaap" | "dei" | "ifrs-full"
                        "concept":      concept_name,    # all concepts, no filter
                        "label":        concept_data.label,
                        "unit":         unit,            # "USD" | "shares" | "pure" | etc.
                        "end":          entry.end,
                        "start":        entry.start,     # None for instant (balance sheet)
                        "val":          entry.val,
                        "accn":         entry.accn,
                        "form":         entry.form,
                        "filed":        entry.filed,
                        "frame":        entry.frame,     # CY2023 / CY2023Q3I / None
                    })
    return rows
