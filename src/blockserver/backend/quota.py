
class QuotaPolicy:

    METAFILE_THRESHOLD = 150 * 1024
    TRAFFIC_THRESHOLD = 100 * 1024**3

    @staticmethod
    def upload(quota_reached, file_size, is_block, is_overwrite=False):
        if not quota_reached:
            return True
        if is_block:
            return False
        return is_overwrite and file_size < QuotaPolicy.METAFILE_THRESHOLD

    @staticmethod
    def download(current_traffic):
        return current_traffic <= QuotaPolicy.TRAFFIC_THRESHOLD

    @staticmethod
    def delete():
        return True
