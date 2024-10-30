"""
Test module for the app config.
"""


def test_archive_api_proxy(
    testing_config
):
    """
    Test the app config when the environment variable
    ARCHIVE_API_PROXY is set.
    """

    proxy = {
        "http": "https://www.lzv.nrw/proxy"
    }

    testing_config.ARCHIVE_API_PROXY = proxy

    assert (testing_config().CONTAINER_SELF_DESCRIPTION["configuration"]
                                                       ["settings"]
                                                       ["ingest"]
                                                       ["proxy"]) == proxy
