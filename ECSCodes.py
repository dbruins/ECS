
ok = b'\x00'
error = b'\x01'
errorCreatingPartition = b'\x14'
errorMapping = b'\x15'


hello = b'\x02'
ping = b'\x03'
idUnknown = b'\x04'

pcaAsksForConfig = b'\x05'
pcaAsksForDetectorList = b'\x06'

detectorAsksForId = b'\x07'

getAllPCAs = b'\x08'
getUnmappedDetectors = b'\x11'
mapDetectorsToPCA = b'\x12'

createPartition = b'\x09'
createDetector = b'\x13'

timeout = b'\x10'
