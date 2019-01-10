from storages.file_storage import FileStorage


class SbStorage(FileStorage):
    def get_loaded_urls(self, storage):
        loaded_urls = []
        if not storage.is_exist():
            return loaded_urls
        for line in storage.read_data():
            data = line.split('\t')
            loaded_urls.append(data[0])
        return loaded_urls
