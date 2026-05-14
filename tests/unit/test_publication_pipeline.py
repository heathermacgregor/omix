from __future__ import annotations

import pandas as pd
import pytest

from omix.publications.pipeline import (
    filter_publications_by_accession,
    populate_publication_fields,
)


@pytest.mark.parametrize("study_accession", ["PRJNA1234075", "PRJEB44414"])
def test_filter_publications_by_accession_keeps_direct_matches(study_accession: str):
    publications = {
        study_accession: [
            {
                "status": "✓ Extraction complete.",
                "matched_queries": [study_accession],
                "accession_mentions_in_text": 0,
                "doi": f"10.1234/{study_accession.lower()}-direct",
            },
            {
                "status": "✓ Extraction complete.",
                "matched_queries": [f'DATA:"{study_accession}"'],
                "accession_mentions_in_text": 0,
                "doi": f"10.1234/{study_accession.lower()}-generic",
            },
            {
                "status": "✓ Extraction complete.",
                "matched_queries": ["unrelated query"],
                "accession_mentions_in_text": 2,
                "doi": f"10.1234/{study_accession.lower()}-text",
            },
        ]
    }

    filtered = filter_publications_by_accession(publications)

    assert list(filtered) == [study_accession]
    assert len(filtered[study_accession]) == 2
    assert {pub["doi"] for pub in filtered[study_accession]} == {
        f"10.1234/{study_accession.lower()}-direct",
        f"10.1234/{study_accession.lower()}-text",
    }


@pytest.mark.parametrize("study_accession", ["PRJNA1234075", "PRJEB44414"])
def test_populate_publication_fields_aggregates_dois(study_accession: str):
    df = pd.DataFrame(
        {
            "study_accession": [study_accession, study_accession, "OTHER"],
            "sample_accession": ["SAMN1", "SAMN2", "SAMN3"],
        }
    )
    publications = {
        study_accession: [
            {"doi": f"10.1234/{study_accession.lower()}-1"},
            {"doi": f"10.1234/{study_accession.lower()}-1"},
            {"publication_doi": f"10.1234/{study_accession.lower()}-2"},
            {"externalIds": {"DOI": f"10.1234/{study_accession.lower()}-3"}},
        ]
    }

    filled_rows, studies_with_dois = populate_publication_fields(df, publications)

    assert filled_rows == 2
    assert studies_with_dois == 1
    assert df.loc[df["study_accession"] == study_accession, "publication_count"].tolist() == [3, 3]
    assert df.loc[df["study_accession"] == study_accession, "publication_dois"].tolist() == [
        f"10.1234/{study_accession.lower()}-1; 10.1234/{study_accession.lower()}-2; 10.1234/{study_accession.lower()}-3",
        f"10.1234/{study_accession.lower()}-1; 10.1234/{study_accession.lower()}-2; 10.1234/{study_accession.lower()}-3",
    ]