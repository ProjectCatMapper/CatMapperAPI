
from dataclasses import dataclass
from typing import List, Dict, Optional
import uuid

# creating classes to hold different datatypes
@dataclass
class MergingTieStackDataset:
    mergingID: str
    stackID: Optional[str]
    datasetID: str

@dataclass
class MergingTieVariable:
    mergingID: str
    stackID: Optional[str]
    datasetID: str
    variableID: str
    key: str
    varName: str
    stackTransform: Optional[str]
    variableFilter: Optional[str]
    summaryStatistic: Optional[str]
    summaryFilter: Optional[str]
    summaryWeight: Optional[str]
    datasetTransform: Optional[str]

@dataclass
class EquivalenceTie:
    mergingID: str
    categoryID: str
    key: str
    datasetID: Optional[str] 


def require(value, message):
    """Raise error if a required field is empty."""
    if value is None or value == "":
        raise ValueError(message)


def validate_exists(value, valid_set, message):
    """Raise error if a referenced ID does not exist in the DB."""
    if value not in valid_set:
        raise ValueError(message)


def create_new_stack_id():
    """Generate unique stackID."""
    return f"s_{uuid.uuid4().hex[:6]}"



def validate_merging_template_inputs(
    merging_stack_dataset: List[MergingTieStackDataset],
    merging_variables: List[MergingTieVariable],
    equivalence_ties: List[EquivalenceTie],
    database_ids: Dict[str, set]
):
    """
    database_ids must contain keys:
      mergingIDs, stackIDs, datasetIDs, variableIDs, categoryIDs
    """

    for tie in merging_stack_dataset:
        require(tie.mergingID, "mergingID is required.")
        require(tie.datasetID, "datasetID is required.")

        validate_exists(
            tie.mergingID,
            database_ids["mergingIDs"],
            f"Unknown mergingID '{tie.mergingID}'"
        )
        validate_exists(
            tie.datasetID,
            database_ids["datasetIDs"],
            f"Unknown datasetID '{tie.datasetID}'"
        )

        if tie.stackID:
            validate_exists(
                tie.stackID,
                database_ids["stackIDs"],
                f"Unknown stackID '{tie.stackID}'"
            )


    for mv in merging_variables:
        require(mv.mergingID, "mergingID required")
        require(mv.datasetID, "datasetID required")
        require(mv.variableID, "variableID required")
        require(mv.key, "key required")

        validate_exists(mv.mergingID, database_ids["mergingIDs"], f"Unknown mergingID '{mv.mergingID}'")
        validate_exists(mv.datasetID, database_ids["datasetIDs"], f"Unknown datasetID '{mv.datasetID}'")
        validate_exists(mv.variableID, database_ids["variableIDs"], f"Unknown variableID '{mv.variableID}'")

        if mv.stackID:
            validate_exists(mv.stackID, database_ids["stackIDs"], f"Unknown stackID '{mv.stackID}'")

    for eq in equivalence_ties:
        require(eq.mergingID, "mergingID required in equivalence tie")
        require(eq.categoryID, "categoryID required in equivalence tie")
        require(eq.key, "key required in equivalence tie")

        validate_exists(eq.mergingID, database_ids["mergingIDs"], f"Unknown mergingID '{eq.mergingID}'")
        validate_exists(eq.categoryID, database_ids["categoryIDs"], f"Unknown categoryID '{eq.categoryID}'")

        # Type2: datasetID exists
        if eq.datasetID:
            validate_exists(eq.datasetID, database_ids["datasetIDs"], f"Unknown datasetID '{eq.datasetID}'")

    return True


def process_type1_updates(
    merging_stack_dataset: List[MergingTieStackDataset],
    db_insert_stack,
    db_insert_merging_stack,
    db_insert_stack_dataset
):
    """
    If stackID is missing → auto-create + create ties:
        mergingID ↔ stackID
        stackID  ↔ datasetID
    """

    for tie in merging_stack_dataset:

        if not tie.stackID:  # TYPE 1
            new_stack = create_new_stack_id()
            tie.stackID = new_stack

            # Insert new stack
            db_insert_stack(new_stack)

            # Create ties
            db_insert_merging_stack(tie.mergingID, new_stack)
            db_insert_stack_dataset(new_stack, tie.datasetID)

        else:
            # Explicit stackID given → ensure ties exist
            db_insert_merging_stack(tie.mergingID, tie.stackID)
            db_insert_stack_dataset(tie.stackID, tie.datasetID)


def process_merging_template(
    merging_stack_dataset: List[MergingTieStackDataset],
    merging_variables: List[MergingTieVariable],
    equivalence_ties: List[EquivalenceTie],
    database_ids: Dict[str, set],
    db_insert_stack,
    db_insert_merging_stack,
    db_insert_stack_dataset,
):
    """
    Main function to run merging template processing.
    """

    # STEP 1 — Validate everything
    validate_merging_template_inputs(
        merging_stack_dataset,
        merging_variables,
        equivalence_ties,
        database_ids
    )

    # STEP 2 — Handle Type1 logic
    process_type1_updates(
        merging_stack_dataset,
        db_insert_stack,
        db_insert_merging_stack,
        db_insert_stack_dataset
    )

    return True


def mock_insert_stack(stackID):
    print(f"[DB] Create stack: {stackID}")

def mock_insert_merging_stack(mergingID, stackID):
    print(f"[DB] Link mergingID={mergingID} to stackID={stackID}")

def mock_insert_stack_dataset(stackID, datasetID):
    print(f"[DB] Link stackID={stackID} to datasetID={datasetID}")


if __name__ == "__main__":

    db_ids = {
        "mergingIDs": {"m1", "m2"},
        "stackIDs": {"s1"},
        "datasetIDs": {"d1", "d2"},
        "variableIDs": {"v1", "v2"},
        "categoryIDs": {"c1", "c2", "c3"}
    }

    merging_stack_ds = [
        MergingTieStackDataset("m1", None, "d1"),
        MergingTieStackDataset("m1", "s1", "d2")  
    ]

    variables = [
        MergingTieVariable("m1", "s1", "d1", "v1", "k1", "test", None, None, "mean", None, None, None)
    ]

    eq_ties = [
        EquivalenceTie("m1", "c2", "k1", "d1"),
        EquivalenceTie("m1", "c3", "k5", None) 
    ]

    process_merging_template(
        merging_stack_ds,
        variables,
        eq_ties,
        db_ids,
        mock_insert_stack,
        mock_insert_merging_stack,
        mock_insert_stack_dataset
    )
