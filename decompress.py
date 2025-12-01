import gzip

volume = '/Volumes/workspace/imdb/imdb_data/compressed/'
save_dir = '/Volumes/workspace/imdb/imdb_data/data/'
dbutils.fs.mkdirs(save_dir)

for file in dbutils.fs.ls(volume):
  print(file.name)
  with gzip.open(volume+file.name, 'rb') as f_in:
    with open(save_dir+file.name[:-3], 'wb') as f_out:
        f_out.write(f_in.read())