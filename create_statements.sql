CREATE TRIGGER deleteMappingOnDetectorDelete AFTER DELETE ON Detector
BEGIN
  DELETE FROM Mapping WHERE OLD.id = Mapping.DetectorId;
END;

CREATE TRIGGER deleteMappingOnPartitionDelete AFTER DELETE ON Partition
BEGIN
  DELETE FROM Mapping WHERE OLD.id = Mapping.DetectorId;
END;

DROP trigger blockDuplicatePorts_detector;
CREATE TRIGGER blockDuplicatePorts_detector BEFORE INSERT ON Detector
BEGIN
  SELECT
    CASE
      WHEN
        EXISTS(SELECT * FROM Detector d join Partition p on d.address = p.address WHERE d.address = New.address AND (d.CommandPort = NEW.CommandPort or p.portPublish = NEW.CommandPort or p.portLog = NEW.CommandPort or p.portUpdates = NEW.CommandPort or p.portCurrentState = NEW.CommandPort or p.portCommand = NEW.CommandPort ))
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
																				   (d.CommandPort = NEW.portPublish or p.portPublish = NEW.portPublish or p.portLog = NEW.portPublish or p.portUpdates = NEW.portPublish or p.portCurrentState = NEW.portPublish or p.portCommand = NEW.portPublish  or
																					d.CommandPort = NEW.portLog or p.portPublish = NEW.portLog or p.portLog = NEW.portLog or p.portUpdates = NEW.portLog or p.portCurrentState = NEW.portLog or p.portCommand = NEW.portLog or
																					d.CommandPort = NEW.portUpdates or p.portPublish = NEW.portUpdates or p.portLog = NEW.portUpdates or p.portUpdates = NEW.portUpdates or p.portCurrentState = NEW.portUpdates or p.portCommand = NEW.portUpdates or
																					d.CommandPort = NEW.portCurrentState or p.portPublish = NEW.portCurrentState or p.portLog = NEW.portCurrentState or p.portUpdates = NEW.portCurrentState or p.portCurrentState = NEW.portCurrentState or p.portCommand = NEW.portCurrentState or
																					d.CommandPort = NEW.portCommand or p.portPublish = NEW.portCommand or p.portLog = NEW.portCommand or p.portUpdates = NEW.portCommand or p.portCurrentState = NEW.portCommand or p.portCommand = NEW.portCommand
																					)) THEN
      RAISE (ABORT,'Duplicate Port for Address')
    END;
END;

DROP TRIGGER assertIdsExist;
CREATE TRIGGER assertIdsExist
  BEFORE INSERT ON Mapping
    BEGIN
	  SELECT CASE
      WHEN
        Not EXISTS (SELECT * FROM Detector WHERE id = NEW.DetectorId)
      then
        RAISE (ABORT,'detector id does not exist')
	  WHEN
        Not EXISTS (SELECT * FROM Partition WHERE id = NEW.PartitionId)
	  then
        RAISE (ABORT,'pca id does not exist')
     END;
	END;

SELECT * FROM Detector ORDER BY (CAST(id AS int));
UPDATE Detector SET address="pn03" WHERE (CAST (id AS int)) > 5;
UPDATE Detector SET address="pn02" WHERE (CAST (id AS int)) <= 5;

CREATE TABLE "ConfigurationTag" ( `TagName` TEXT NOT NULL, `configId` TEXT NOT NULL, PRIMARY KEY(`TagName`,`configId`) )
CREATE TABLE "Configurations" ( `configId` TEXT NOT NULL, `systemId` TEXT NOT NULL, `Parameters` JSON, PRIMARY KEY(`configId`) )


INSERT INTO ConfigurationTag VALUES ("tagX","test1"),("tagX","test3"),("tagX","test4"),("tagX","testDCS"),("tagX","testFLES2"),("tagX","testQA"),("tagX","testTFC"),("tagX","Beispiel");

CREATE TRIGGER configExists BEFORE INSERT ON ConfigurationTag
    BEGIN
  	  SELECT CASE
        WHEN
          Not EXISTS (SELECT * FROM Configurations WHERE configId = NEW.configId)
        then
          RAISE (ABORT,'Configuration does not exist')
    END;
END;

CREATE TRIGGER deleteTagOnConfigDelete AFTER DELETE ON Configurations
BEGIN
  --DELETE FROM ConfigurationTag WHERE ConfigurationTag.TagName IN (SELECT tagName FROM ConfigurationTag WHERE ConfigurationTag.configId = OLD.configId);
  DELETE FROM ConfigurationTag WHERE ConfigurationTag.configId = OLD.configId;
END;

CREATE TRIGGER deleteConfigOnDetectorDelete AFTER DELETE ON Detector
BEGIN
  DELETE FROM Configurations WHERE Configurations.systemId = OLD.id;
END;
