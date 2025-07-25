"""ImportSource-data model test-module."""

from dcm_common.models.data_model import get_model_serialization_test

from dcm_backend.models import ImportSource


test_import_source_json = get_model_serialization_test(
    ImportSource,
    (
        ((), {"name": "n", "path": "p", "id_": "0"}),
        (
            (),
            {
                "name": "n",
                "path": "p",
                "id_": "0",
                "description": "some description",
            },
        ),
    ),
)


def test_import_source_row_from_row():
    """Test database-serialization."""
    row = ImportSource("id0", "name", "path", "description").row
    assert ImportSource.from_row(row).row == row
