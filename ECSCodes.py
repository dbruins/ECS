
ok = b'\x00'
error = b'\x01'
errorCreatingPartition = b'\x14'
errorMapping = b'\x15'
detectorChangePartition = b'\x31'
unknownCommand = b'\x27'
reset = b'\x29'
removed = b'\x33'
done = b'\x30'
addDetector = b'\x33'
removeDetector = b'\x32'

hello = b'\x02'
ping = b'\x03'
idUnknown = b'\x04'

pcaAsksForConfig = b'\x05'
pcaAsksForDetectorList = b'\x06'
pcaAsksForDetectorStatus = b'\x22'

detectorAsksForId = b'\x07'
detectorAsksForPCA = b'\x25'

getAllPCAs = b'\x08'
getPartitionForId = b'\x23'
getDetectorForId = b'\x26'
getUnmappedDetectors = b'\x11'
mapDetectorsToPCA = b'\x12'

createPartition = b'\x09'
createDetector = b'\x13'

timeout = b'\x10'

shutdown = b'\x16'
getReady = b'\x17'
start = b'\x18'
stop = b'\x19'

setActive = b'\x20'
setInactive = b'\x21'
