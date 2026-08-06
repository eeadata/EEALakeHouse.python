import pytest

from eea_datalakehouse.data_preparation import DataAcquirer, Explorer, VocabularyLoader


@pytest.mark.parametrize("base", [DataAcquirer, VocabularyLoader, Explorer])
def test_stage_base_cannot_be_instantiated_directly(base):
    with pytest.raises(TypeError):
        base()
