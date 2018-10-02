class PCAStates:
    Idle = "Idle"
    Configuring_TFC = "Configuring_TFC"
    TFC_Configured = "TFC_Configured"
    Configuring_Detectors = "Configuring_Detectors"
    Detectors_Configured = "Detectors_Configured"
    Configuring_FLES_and_DCS = "Configuring_FLES_and_DCS"
    FLES_and_DCS_Configured = "FLES_and_DCS_Configured"
    Configuring_QA = "Configuring_QA"
    QA_Configured = "QA_Configured"
    Recording = "Recording"

class PCATransitions:
    success = "success"
    fail = "fail"
    abort = "abort"
    configure = "configure"
    error_TFC = "error_TFC"
    error_Detector = "error_Detector"
    error_FLES_OR_DCS = "error_FLES_OR_DCS"
    error_QA = "error_QA"
    start_recording = "start_recording"

class MappedStates:
    Active = "Active"
    Configuring = "Configuring"
    Unconfigured = "Unconfigured"

class DetectorStates:
    Unconfigured = "Unconfigured"
    Configuring_Step1 = "Configuring_Step1"
    Configuring_Step2 = "Configuring_Step2"
    Active = "Active"
    ConnectionProblem = "Connection Problem"
    Inactive = "Inactive"

class DetectorTransitions:
    success = "success"
    fail = "fail"
    abort = "abort"
    configure = "configure"
    error = "error"

class GlobalSystemStates:
    Unconfigured = "Unconfigured"
    Configuring = "Configuring"
    Active = "Active"
    ConnectionProblem = "Connection Problem"

class GlobalSystemStatesTransitions:
    success = "success"
    fail = "fail"
    abort = "abort"
    configure = "configure"
    error = "error"

class TFCStates(GlobalSystemStates):
    pass

class TFCTransitions(GlobalSystemStatesTransitions):
    pass

class DCSStates(GlobalSystemStates):
    pass

class DCSTransitions(GlobalSystemStatesTransitions):
    pass

class QAStates(GlobalSystemStates):
    Recording = "Recording"

class QATransitions(GlobalSystemStatesTransitions):
    start = "start"
    stop = "stop"

class FLESStates(GlobalSystemStates):
    Recording = "Recording"

class FLESTransitions(GlobalSystemStatesTransitions):
    start = "start"
    stop = "stop"
