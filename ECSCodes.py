class ECSCodes:


    ok = b'\x00'
    error = b'\x01'
    errorCreatingPartition = b'\x14'
    errorMapping = b'\x15'
    detectorChangePartition = b'\x31'
    unknownCommand = b'\x27'

    reset = b'\x29'
    removed = b'\x35'

    PCAAsksForTransitionsStatus = b'\x34'
    busy = b'\x36'
    done = b'\x30'
    addDetector = b'\x33'
    removeDetector = b'\x32'
    check = b'\x38'
    deleteDetector = b'\x39'
    deletePartition = b'\x43'
    connectionProblemDetector = b'\x40'
    connectionProblemOldPartition = b'\x41'
    connectionProblemNewPartition = b'\x42'

    hello = b'\x02'
    ping = b'\x03'
    idUnknown = b'\x04'

    pcaAsksForConfig = b'\x05'
    pcaAsksForDetectorList = b'\x06'
    pcaAsksForDetectorStatus = b'\x22'

    detectorAsksForId = b'\x07'
    detectorAsksForPCA = b'\x25'

    GlobalSystemAsksForInfo = b'\x46'

    getAllPCAs = b'\x08'
    getPartitionForId = b'\x23'
    getDetectorForId = b'\x26'
    getUnmappedDetectors = b'\x11'
    mapDetectorsToPCA = b'\x12'
    getDetectorMapping = b'\x47'

    createPartition = b'\x09'
    createDetector = b'\x13'

    timeout = b'\x10'

    getReady = b'\x17'
    start = b'\x18'
    abort = b'\x37'

    setActive = b'\x20'
    setInactive = b'\x21'

    def stringForCode(self,code):
        if code in self.stringForStatusCode:
            return self.stringForStatusCode[code]
        else:
            return "No known String for code %s" % code

    stringForStatusCode = {
        start : "start",
        ok : "ok",
        error : "error",
        errorCreatingPartition : "error while creating Partition",
        errorMapping : "error while mapping Detector",
        detectorChangePartition: "detector has a new Partition",
        unknownCommand : "command is unknown",

        reset: "start/restart",

        busy: "is already in a Transition",
        idUnknown: "id is unknown",

        getReady: "configure",
        start: "start",
        abort: "abort",
    }
