
from wsgidav.dav_error import DAVError, HTTP_FORBIDDEN
from wsgidav.dav_provider import DAVProvider, DAVCollection, DAVNonCollection

from datetime import datetime
import wsgidav.util as util
import os
import mimetypes
import shutil
import stat
import tempfile

from client.nibbler import Nibbler
from client.metadata import DirectoryMD, FileMD

__docformat__ = "reStructuredText"

_logger = util.getModuleLogger(__name__)

BUFFER_SIZE = 8192


#===============================================================================
# FileResource
#===============================================================================
class FileResource(DAVNonCollection):
    """Represents a single existing DAV resource instance.

    See also _DAVResource, DAVNonCollection, and FilesystemProvider.
    """
    def __init__(self, nibbler, path, environ, file_obj):
        super(FileResource, self).__init__(path, environ)
        self.nibbler = nibbler
        self.file_obj = file_obj

        # Setting the name from the file path should fix the case on Windows
        self.name = os.path.basename(file_obj.name)
        self.name = self.name.encode("utf8")

        self._filePath = None

    # Getter methods for standard live properties     
    def getContentLength(self):
        return self.file_obj.size

    def getContentType(self):
        return 'application/octet-stream'

        #mimetype = self.file_obj.mimetype
        #if not mimetype:
        #    mimetype = "application/octet-stream"
        #return mimetype

    def _to_unix_time(self, date):
        return float(datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ').strftime("%s"))

    def getCreationDate(self):
        return self._to_unix_time(self.file_obj.create_date)

    def getDisplayName(self):
        return self.name

    def getEtag(self):
        return util.getETag(self.file_obj.name)

    def getLastModified(self):
        return self._to_unix_time(self.file_obj.create_date)

    def supportEtag(self):
        return True

    def supportRanges(self):
        return True

    def getContent(self):
        """Open content as a stream for reading.

        See DAVResource.getContent()
        """
        return self.nibbler.load_file(self.file_obj)

    def beginWrite(self, contentType=None):
        """Open content as a stream for writing.

        See DAVResource.beginWrite()
        """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)

        f_idx, tmpfl = tempfile.mkstemp(prefix='nibbler-upload')
        f_obj = os.fdopen(f_idx, "wb")
        self._filePath = tmpfl
        return f_obj

    def endWrite(self, withErrors):
        """Called when PUT has finished writing.

        This is only a notification. that MAY be handled.
        """
        if not withErrors:
            self.nibbler.save_file(self._filePath, self.file_obj, \
                        os.path.dirname(self.path).decode('utf8'))

        if self._filePath:
            os.unlink(self._filePath)
            self._filePath = None


    def delete(self):
        """Remove this resource or collection (recursive).

        See DAVResource.delete()
        """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)

        self.nibbler.remove_file(self.path.decode('utf8'))

        self.removeAllProperties(True)
        self.removeAllLocks(True)

    def copyMoveSingle(self, destPath, isMove):
        """See DAVResource.copyMoveSingle() """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)

        fpDest = self.provider._locToFilePath(destPath)
        assert not util.isEqualOrChildUri(self.path, destPath)
        # Copy file (overwrite, if exists)
        shutil.copy2(self._filePath, fpDest)
        # (Live properties are copied by copy2 or copystat)
        # Copy dead properties
        propMan = self.provider.propManager
        if propMan:
            destRes = self.provider.getResourceInst(destPath, self.environ)
            if isMove:
                propMan.moveProperties(self.getRefUrl(), destRes.getRefUrl(), 
                                       withChildren=False)
            else:
                propMan.copyProperties(self.getRefUrl(), destRes.getRefUrl())
               

    def supportRecursiveMove(self, destPath):
        """Return True, if moveRecursive() is available (see comments there)."""
        return True

    
    def moveRecursive(self, destPath):
        """See DAVResource.moveRecursive() """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)               
        fpDest = self.provider._locToFilePath(destPath)
        assert not util.isEqualOrChildUri(self.path, destPath)
        assert not os.path.exists(fpDest)
        _logger.debug("moveRecursive(%s, %s)" % (self._filePath, fpDest))
        shutil.move(self._filePath, fpDest)
        # (Live properties are copied by copy2 or copystat)
        # Move dead properties
        if self.provider.propManager:
            destRes = self.provider.getResourceInst(destPath, self.environ)
            self.provider.propManager.moveProperties(self.getRefUrl(), destRes.getRefUrl(), 
                                                     withChildren=True)
               


    
#===============================================================================
# FolderResource
#===============================================================================
class FolderResource(DAVCollection):
    """Represents a single existing file system folder DAV resource.

    See also _DAVResource, DAVCollection, and FilesystemProvider.
    """
    def __init__(self, nibbler, path, environ, dir_obj):
        super(FolderResource, self).__init__(path, environ)

        self.nibbler = nibbler
        self.dir_obj = dir_obj

        # Setting the name from the file path should fix the case on Windows
        self.name = os.path.basename(self.dir_obj.name)
        self.name = self.name.encode("utf8")


    # Getter methods for standard live properties     

    def _to_unix_time(self, date):
        return float(datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ').strftime("%s"))

    def getCreationDate(self):
        return self._to_unix_time(self.dir_obj.create_date)

    def getDisplayName(self):
        return self.name

    def getDirectoryInfo(self):
        return None

    def getEtag(self):
        return None

    def getLastModified(self):
        return self._to_unix_time(self.dir_obj.last_modify_date)

    def getMemberNames(self):
        """Return list of direct collection member names (utf-8 encoded).

        See DAVCollection.getMemberNames()
        """
        # On Windows NT/2k/XP and Unix, if path is a Unicode object, the result 
        # will be a list of Unicode objects. 
        # Undecodable filenames will still be returned as string objects    
        # If we don't request unicode, for example Vista may return a '?' 
        # instead of a special character. The name would then be unusable to
        # build a distinct URL that references this resource.

        nameList = []

        for name, is_file in self.dir_obj.items():
            name = name.encode("utf8")
            nameList.append(name)

        return nameList

    def getMember(self, name):
        """Return direct collection member (DAVResource or derived).

        See DAVCollection.getMember()
        """
        r_obj = self.dir_obj.get(name.decode("utf8"))

        path = util.joinUri(self.path, name)
        if r_obj.is_dir():
            res = FolderResource(self.nibbler, path, self.environ, r_obj)
        else:
            res = FileResource(self.nibbler, path, self.environ, r_obj)

        return res



    # --- Read / write ---------------------------------------------------------
    def createEmptyResource(self, name):
        """Create an empty (length-0) resource.

        See DAVResource.createEmptyResource()
        """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)

        if name.startswith('.'):
            raise DAVError(HTTP_FORBIDDEN)

        path = util.joinUri(self.path, name)
        file_md = FileMD(name.decode('utf8'))
        self.dir_obj.append(file_md)

        return self.provider.getResourceInst(path, self.environ)


    def createCollection(self, name):
        """Create a new collection as member of self.

        See DAVResource.createCollection()
        """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)

        dir_md = DirectoryMD(name.decode('utf8'))
        self.dir_obj.append(dir_md)


    def delete(self):
        """Remove this resource or collection (recursive).

        See DAVResource.delete()
        """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)

        self.nibbler.rmdir(self.path.rstrip('/').decode('utf8'), recursive=True)

        self.removeAllProperties(True)
        self.removeAllLocks(True)


    def copyMoveSingle(self, destPath, isMove):
        """See DAVResource.copyMoveSingle() """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)
        fpDest = self.provider._locToFilePath(destPath)
        assert not util.isEqualOrChildUri(self.path, destPath)
        # Create destination collection, if not exists
        if not os.path.exists(fpDest):
            os.mkdir(fpDest)
        try:
            # may raise: [Error 5] Permission denied: u'C:\\temp\\litmus\\ccdest'
            shutil.copystat(self._filePath, fpDest)
        except Exception, e:
            _logger.debug("Could not copy folder stats: %s" % e)
        # (Live properties are copied by copy2 or copystat)
        # Copy dead properties
        propMan = self.provider.propManager
        if propMan:
            destRes = self.provider.getResourceInst(destPath, self.environ)
            if isMove:
                propMan.moveProperties(self.getRefUrl(), destRes.getRefUrl(),
                                       withChildren=False)
            else:
                propMan.copyProperties(self.getRefUrl(), destRes.getRefUrl())


    def supportRecursiveMove(self, destPath):
        """Return True, if moveRecursive() is available (see comments there)."""
        return True


    def moveRecursive(self, destPath):
        """See DAVResource.moveRecursive() """
        if self.provider.readonly:
            raise DAVError(HTTP_FORBIDDEN)               
        fpDest = self.provider._locToFilePath(destPath)
        assert not util.isEqualOrChildUri(self.path, destPath)
        assert not os.path.exists(fpDest)
        _logger.debug("moveRecursive(%s, %s)" % (self._filePath, fpDest))
        shutil.move(self._filePath, fpDest)
        # (Live properties are copied by copy2 or copystat)
        # Move dead properties
        if self.provider.propManager:
            destRes = self.provider.getResourceInst(destPath, self.environ)
            self.provider.propManager.moveProperties(self.getRefUrl(), destRes.getRefUrl(), 
                                                     withChildren=True)


#===============================================================================
# FabnetProvider
#===============================================================================
class FabnetProvider(DAVProvider):
    def __init__(self, fabnet_host, security_provider):
        super(FabnetProvider, self).__init__()
        self.nibbler = Nibbler(fabnet_host, security_provider)
        self.readonly = False

    def getResourceInst(self, path, environ):
        """Return info dictionary for path.

        See DAVProvider.getResourceInst()
        """
        self._count_getResourceInst += 1
        fp = util.toUnicode(path.rstrip("/"))
        r_obj = self.nibbler.get_resource(fp)

        if r_obj is None:
            return None

        if r_obj.is_dir():
            return FolderResource(self.nibbler, path, environ, r_obj)

        return FileResource(self.nibbler, path, environ, r_obj)
