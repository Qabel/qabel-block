

def test_quota_reached_block_upload_denied(quota_policy):
    assert not quota_policy.upload(10, 10, 1, True, False)
    assert not quota_policy.upload(10, 10, 1, True)
    assert not quota_policy.upload(10, 10, 0, True)


def test_quota_not_reached_block_upload_granted(quota_policy):
    assert quota_policy.upload(10, 0, 10, True)
    assert quota_policy.upload(10, 1, 9, True)


def test_quota_not_reached_block_upload_denied(quota_policy):
    assert not quota_policy.upload(10, 0, 11, True)
    assert not quota_policy.upload(1, 0, 2, True)


def test_quota_reached_meta_upload_denied(quota_policy):
    assert not quota_policy.upload(0, 0, 0, False)
    assert not quota_policy.upload(0, 10, 0, False)
    assert not quota_policy.upload(10, 10, 0, False)
    assert not quota_policy.upload(10, 10, 151*1024, False, True)


def test_quota_reached_meta_upload_granted(quota_policy):
    assert quota_policy.upload(10, 10, 10, False, True)
    assert quota_policy.upload(10, 10, 0, False, True)
    assert not quota_policy.upload(10, 10, 150*1024, False, True)


def test_traffic_limit(quota_policy):
    quota_policy.TRAFFIC_THRESHOLD = 10
    assert quota_policy.download(10)
    assert not quota_policy.download(11)


def test_delete_policy(quota_policy):
    assert quota_policy.delete()
