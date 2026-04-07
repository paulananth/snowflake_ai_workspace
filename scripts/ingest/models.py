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

from typing import Optional
from pydantic import BaseModel, model_validator


# ── Submissions ───────────────────────────────────────────────────────────────

class Address(BaseModel):
    street1:                    Optional[str] = None
    street2:                    Optional[str] = None
    city:                       Optional[str] = None
    stateOrCountry:             Optional[str] = None
    zipCode:                    Optional[str] = None
    stateOrCountryDescription:  Optional[str] = None


class FormerName(BaseModel):
    name: str
    date: str


class Filing(BaseModel):
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
    size:                   Optional[int] = None
    isXBRL:                 Optional[int] = None
    isInlineXBRL:           Optional[int] = None
    primaryDocument:        Optional[str] = None
    primaryDocDescription:  Optional[str] = None


class FilingsFile(BaseModel):
    """Entry in filings.files — pointer to an older submissions batch."""
    name:         str
    filingCount:  int
    filingFrom:   str
    filingTo:     str


class Submission(BaseModel):
    """
    Full company submission record from SEC EDGAR.
    Maps every top-level field returned by the submissions endpoint.
    filings.recent is NOT stored here — use explode_filings() to get Filing rows.
    """
    cik:                                str
    entityType:                         Optional[str] = None
    sic:                                Optional[str] = None
    sicDescription:                     Optional[str] = None
    insiderTransactionForOwnerExists:   Optional[int] = None
    insiderTransactionForIssuerExists:  Optional[int] = None
    name:                               str
    tickers:                            list[str] = []
    exchanges:                          list[str] = []
    ein:                                Optional[str] = None
    description:                        Optional[str] = None
    website:                            Optional[str] = None
    investorWebsite:                    Optional[str] = None
    category:                           Optional[str] = None
    fiscalYearEnd:                      Optional[str] = None
    stateOfIncorporation:               Optional[str] = None
    stateOfIncorporationDescription:    Optional[str] = None
    phone:                              Optional[str] = None
    flags:                              Optional[str] = None
    formerNames:                        list[FormerName] = []
    addresses:                          Optional[dict[str, Address]] = None
    filing_files:                       list[FilingsFile] = []

    @model_validator(mode="before")
    @classmethod
    def _extract_filing_files(cls, data: dict) -> dict:
        """Pull filings.files up to top-level before validation."""
        filings = data.get("filings") or {}
        data["filing_files"] = filings.get("files") or []
        return data


# ── CompanyFacts ──────────────────────────────────────────────────────────────

class FactEntry(BaseModel):
    """
    One tagged value from an XBRL filing.
    start is absent for instant facts (balance sheet items at a point in time).
    frame is present when SEC has mapped the entry to a standard calendar period.
    """
    accn:   str
    end:    str
    start:  Optional[str] = None   # absent for instant facts
    val:    float
    form:   str
    filed:  str
    frame:  Optional[str] = None   # e.g. "CY2023", "CY2023Q3I", "CY2023Q3"


class ConceptData(BaseModel):
    """All reported values for one XBRL concept (e.g. us-gaap/Revenues)."""
    label:        str
    description:  Optional[str] = None
    units:        dict[str, list[FactEntry]]   # unit → entries, e.g. "USD" → [...]


class CompanyFacts(BaseModel):
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
        row["cik"] = cik10
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
