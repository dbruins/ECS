class PCAStates:
    Idle = "Idle"
    Configuring_TFC = "Configuring_TFC"
    TFC_Active = "TFC_Active"
    Configuring_Detectors = "Configuring_Detectors"
    Detectors_Active = "Detectors_Active"
    Configuring_FLES_and_DCS = "Configuring_FLES_and_DCS"
    FLES_and_DCS_Active = "FLES_and_DCS_Active"
    Configuring_QA = "Configuring_QA"
    QA_Active = "QA_Active"
    Recording = "Recording"

    #states in which PCA is configuring something
    configuringStates = {Configuring_TFC,Configuring_Detectors,Configuring_FLES_and_DCS,Configuring_QA}

    #states in which configuring button should be enabled
    configuringEnabled = {Idle,TFC_Active,Detectors_Active,FLES_and_DCS_Active}
    startEnabled = {QA_Active}
    stopEnabled = {Recording}

    def isConfiguringState(self,state):
        return state in self.configuringStates

    def isActiveState(self,state):
        """checks wether PCA is in a configuring state or Recording State"""
        return state in self.configuringStates.union([self.Recording])

    def UIButtonsForState(self,state):
        ret = {
            "configure": state in self.configuringEnabled,
            "start": state in self.startEnabled,
            "stop": state in self.stopEnabled,
        }
        return ret


class PCATransitions:
    success = "success"
    failure = "failure"
    abort = "abort"
    configure = "configure"
    error_TFC = "error_TFC"
    error_Detector = "error_Detector"
    error_FLES_OR_DCS = "error_FLES_OR_DCS"
    error_QA = "error_QA"
    start_recording = "start_recording"
    stop_recording = "stop_recording"

class MappedStates:
    Active = "Active"
    Configuring = "Configuring"
    Unconfigured = "Unconfigured"

class CommonStates:
    ConnectionProblem = "Connection Problem"

class DetectorStates:
    Unconfigured = "Unconfigured"
    Configuring_Step1 = "Configuring_Step1"
    Configuring_Step2 = "Configuring_Step2"
    Active = "Active"
    Error = "Error"

class DetectorTransitions:
    success = "success"
    abort = "abort"
    configure = "configure"
    error = "error"
    resolved = "resolved"
    reset = "reset"

class GlobalSystemStates:
    Unconfigured = "Unconfigured"
    Configuring = "Configuring"
    Active = "Active"
    Error = "Error"

class GlobalSystemTransitions:
    success = "success"
    abort = "abort"
    configure = "configure"
    error = "error"
    resolved = "resolved"
    reset = "reset"

class TFCStates(GlobalSystemStates):
    pass

class TFCTransitions(GlobalSystemTransitions):
    pass

class DCSStates(GlobalSystemStates):
    pass

class DCSTransitions(GlobalSystemTransitions):
    pass

class QAStates(GlobalSystemStates):
    Recording = "Recording"

class QATransitions(GlobalSystemTransitions):
    start = "start"
    stop = "stop"

class FLESStates(GlobalSystemStates):
    Recording = "Recording"

class FLESTransitions(GlobalSystemTransitions):
    start = "start"
    stop = "stop"
