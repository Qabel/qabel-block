

def test_quota_reached_block_upload_denied(quota_policy):
    assert not quota_policy.upload(True, 1, True, False)
    assert not quota_policy.upload(True, 1, True)
    assert not quota_policy.upload(True, 0, True)


def test_quota_not_reached_block_upload_granted(quota_policy):
    assert quota_policy.upload(False, 10, True)
    assert quota_policy.upload(False, 9, True)


def test_quota_reached_meta_upload_denied(quota_policy):
    assert not quota_policy.upload(True, 0, 0, False)
    assert not quota_policy.upload(True, 10, 0, False)
    assert not quota_policy.upload(True, 0, False)
    assert not quota_policy.upload(True, 151*1024, False, True)


def test_quota_reached_meta_upload_granted(quota_policy):
    assert quota_policy.upload(True, 10, False, True)
    assert quota_policy.upload(True, 0, False, True)
    assert not quota_policy.upload(True, 150*1024, False, True)


def test_traffic_limit(quota_policy):
    quota_policy.TRAFFIC_THRESHOLD = 10
    assert quota_policy.download(10)
    assert not quota_policy.download(11)


def test_delete_policy(quota_policy):
    assert quota_policy.delete()
