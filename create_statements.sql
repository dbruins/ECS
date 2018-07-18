CREATE TRIGGER blockDuplicatePorts BEFORE INSERT ON Partition
BEGIN
 SELECT
 CASE
 --checks whether there is duplicate port for one address
 WHEN EXISTS(SELECT * FROM Partition WHERE address = NEW.address AND
   --todo theres more to it e.g. portPublish could be equal to portlOG
	(portPublish = NEW.portPublish OR
	portLog = NEW.portLog OR
	portUpdates = NEW.portUpdates OR
	portCurrentState = NEW.portCurrentState OR
	portSingleRequest = NEW.portSingleRequest OR
	portCommand = NEW.portCommand)) THEN
	RAISE (ABORT,'Duplicate Port for Address')
 END;
END;

CREATE TRIGGER blockDuplicatePorts_detector BEFORE INSERT ON Detector
BEGIN
  SELECT
   CASE
    WHEN
     EXISTS(SELECT * FROM Detector WHERE address = NEW.address AND Port = NEW.Port) THEN
      RAISE (ABORT,'Duplicate Port for Address')
    END;
END;

CREATE TRIGGER deleteMappingOnDetectorDelete AFTER DELETE ON Detector
BEGIN
  DELETE FROM Mapping WHERE OLD.id = Mapping.DetectorId;
END;

CREATE TRIGGER deleteMappingOnPartitionDelete AFTER DELETE ON Partition
BEGIN
  DELETE FROM Mapping WHERE OLD.id = Mapping.DetectorId;
END;
