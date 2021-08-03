import cf.gcs.matchers.lite
from cf.define.fn import GCPCloudFunctionCatalog
from cf.gcs.matchers.catalogs import GCSBlobPathPrefixMatcherCatalog
from cf.gcs.matchers.ext import GCSBlobPathFilenameExtMatcher

prefix = GCSBlobPathPrefixMatcherCatalog()
fn = GCPCloudFunctionCatalog()
ext = GCSBlobPathFilenameExtMatcher
blob_path_matcher = cf.gcs.matchers.lite.blob_path_matcher
