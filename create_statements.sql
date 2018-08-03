CREATE TRIGGER deleteMappingOnDetectorDelete AFTER DELETE ON Detector
BEGIN
  DELETE FROM Mapping WHERE OLD.id = Mapping.DetectorId;
END;

CREATE TRIGGER deleteMappingOnPartitionDelete AFTER DELETE ON Partition
BEGIN
  DELETE FROM Mapping WHERE OLD.id = Mapping.DetectorId;
END;


CREATE TRIGGER blockDuplicatePorts_detector BEFORE INSERT ON Detector
BEGIN
  SELECT
    CASE
      WHEN
        NEW.Port = NEW.PingPort
      then
        RAISE (ABORT,'Identical Ports for Insert')
      WHEN
        EXISTS(SELECT * FROM Detector d join Partition p on d.address = p.address WHERE d.address = New.address AND (d.Port = NEW.Port or d.PingPort = NEW.Port or p.portPublish = NEW.Port or p.portLog = NEW.Port or p.portUpdates = NEW.Port or p.portCurrentState = NEW.Port or p.portCommand = NEW.Port or
                                                                                                                    d.Port = NEW.PingPort or d.PingPort = NEW.PingPort or p.portPublish = NEW.PingPort or p.portLog = NEW.PingPort or p.portUpdates = NEW.PingPort or p.portCurrentState = NEW.PingPort or p.portCommand = NEW.PingPort ))
      THEN
        RAISE (ABORT,'Duplicate Port for Address')
     END;
END

DROP trigger blockDuplicatePorts_partition;
CREATE TRIGGER blockDuplicatePorts_partition BEFORE INSERT ON Partition
BEGIN
  SELECT
   CASE
	WHEN
		NEW.portPublish = New.portLog or NEW.portPublish = New.portUpdates or NEW.portPublish = New.portCurrentState or NEW.portPublish = New.portCommand or
		NEW.portLog = New.portUpdates or NEW.portLog = New.portCurrentState or NEW.portLog = New.portCommand or
		New.portUpdates = New.portCurrentState or New.portUpdates = New.portCommand or
		New.portCurrentState = New.portCommand
	THEN
		RAISE (ABORT,'Duplicate Ports in Insert')
    WHEN
     EXISTS(SELECT * FROM Detector d join Partition p on d.address = p.address  WHERE d.address = New.address AND
																				   (d.Port = NEW.portPublish or d.PingPort = NEW.portPublish or p.portPublish = NEW.portPublish or p.portLog = NEW.portPublish or p.portUpdates = NEW.portPublish or p.portCurrentState = NEW.portPublish or p.portCommand = NEW.portPublish  or
																					d.Port = NEW.portLog or d.PingPort = NEW.portLog or p.portPublish = NEW.portLog or p.portLog = NEW.portLog or p.portUpdates = NEW.portLog or p.portCurrentState = NEW.portLog or p.portCommand = NEW.portLog or
																					d.Port = NEW.portUpdates or d.PingPort = NEW.portUpdates or p.portPublish = NEW.portUpdates or p.portLog = NEW.portUpdates or p.portUpdates = NEW.portUpdates or p.portCurrentState = NEW.portUpdates or p.portCommand = NEW.portUpdates or
																					d.Port = NEW.portCurrentState or d.PingPort = NEW.portCurrentState or p.portPublish = NEW.portCurrentState or p.portLog = NEW.portCurrentState or p.portUpdates = NEW.portCurrentState or p.portCurrentState = NEW.portCurrentState or p.portCommand = NEW.portCurrentState or
																					d.Port = NEW.portCommand or d.PingPort = NEW.portCommand or p.portPublish = NEW.portCommand or p.portLog = NEW.portCommand or p.portUpdates = NEW.portCommand or p.portCurrentState = NEW.portCommand or p.portCommand = NEW.portCommand
																					)) THEN
      RAISE (ABORT,'Duplicate Port for Address')
    END;
END;
