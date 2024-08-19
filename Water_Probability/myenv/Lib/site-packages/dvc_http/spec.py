from fsspec.implementations import http


class HTTPFileSystem(http.HTTPFileSystem):  # pylint: disable=abstract-method
    def __init__(self, *args, upload_method: str = "post", **kwargs):
        super().__init__(*args, **kwargs)
        self.upload_method = upload_method

    async def _put_file(self, *args, **kwargs):
        kwargs.setdefault("method", self.upload_method)
        return await super()._put_file(*args, **kwargs)
