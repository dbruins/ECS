# ECS Prototype for the CBM Experiment
This project provides an Experiment Control System Prototype for the CBM Experiment

## Dependencies:
*  python3.7
*  django
*  zmq
*  paramiko
*  django-guardian
*  django-widget-tweaks
*  zc.lockfile
*  sqlite3

## ECA and Webserver
The Experiment-Control-Agent(ECA) is the highest process on the ECS hierarchie and controlls the entire Experiment. It manages a set Partition-Control-Agents(PCA) and the database for configurations and detector partition-assignments. It provides a user interface via a webserver and the django framework.
The ECA runs as a subprocess of the Webserver. Settings like options or ports for the ECA and Webserver can be found in ``Django/ECS_GUI/ECS_GUI/settings.py``.

The ECS components (ECA,PCA,subsystem controller) can be run distributed on several computing nodes. The components get connection data from the ECS database therefore they need the address and communication ports of the ECS. The Hostaddress and Ports of the ECA/Webserver must be set in ``Django/ECS_GUI/ECS_GUI/settings.py`` and in the ``init.cfg``.

Django includes a lightweight webserver for debugging purposes it can be run via ``python3 Django/ECS_GUI/manage.py runserver``.

For non-debugging usage the django webserver should not be used, instead the Application should be deployed over WSGI on an apache or nginx server with SSL Encryption.

## Deploying on apache2 with ssl encryption:
* copy Project to e.g. /var/www/ECS
* create a SSL Certificate e.g. ``/etc/apache2/ssl/crt/apache.crt`` and a Key e.g. ``/etc/apache2/ssl/key/apache.key``
* create a file /etc/apache2/sites-available/ECS.conf and add something like this:

```
<VirtualHost *:80>
        #redirect all HTTP requests to HTTPS
        ServerName localhost
        ServerAdmin admin@example.com

        # Redirect Requests to SSL
        Redirect permanent "/" "https://localhost/"

	ErrorLog ${APACHE_LOG_DIR}/errorECS.log
        CustomLog ${APACHE_LOG_DIR}/accessECS.log combined

</VirtualHost>

<VirtualHost *:443>
	ServerName localhost

	ServerAdmin webmaster@localhost
	DocumentRoot /var/www/ECS

	# Available loglevels: trace8, ..., trace1, debug, info, notice, warn,
	# error, crit, alert, emerg.
	# It is also possible to configure the loglevel for particular
	# modules, e.g.
	#LogLevel info ssl:warn

	ErrorLog ${APACHE_LOG_DIR}/errorECS.log
	CustomLog ${APACHE_LOG_DIR}/accessECS.log combined

        SSLEngine on

        SSLCertificateFile /etc/apache2/ssl/crt/apache.crt
        SSLCertificateKeyFile /etc/apache2/ssl/key/apache.key

	# For most configuration files from conf-available/, which are
	# enabled or disabled at a global level, it is possible to
	# include a line for only one particular virtual host. For example the
	# following line enables the CGI configuration for this host only
	# after it has been globally disabled with "a2disconf".
	#Include conf-available/serve-cgi-bin.conf
	WSGIScriptAlias / /var/www/ECS/Django/ECS_GUI/ECS_GUI/wsgi.py
	WSGIDaemonProcess test python-home=<Path to virtuel python enviroment> python-path=/var/www/ECS/Django/ECS_GUI/ home=/var/www/ECS/ user=<username>
	WSGIProcessGroup test

	<Directory /var/www/ECS/Django/ECS_GUI/ECS_GUI/>
		<Files wsgi.py>
			Require all granted
		</Files>
	</Directory>

	Alias /static/ /var/www/ECS/Django/ECS_GUI/GUI/static/

	<Directory /var/www/ECS/Django/ECS_GUI/GUI/static/>
		Require all granted
	</Directory>

</VirtualHost>

```
* <Path to virtuel python enviroment> and <username> must be changed acordingly
* run ``sudo a2ensite ECS`` and ``sudo systemctl reload apache2``
* in ECS/Django/ECS_GUI/ECS_GUI/settings.py set ``PATH_TO_PROJECT = "/var/www/ECS"`` and PATH and ``LOG_PATH_ECS = "/var/www/ECS/logECS"`` and ``ENCRYPT_WEBSOCKET=False`` and ``SSL_CERT_FILE="/etc/apache2/ssl/crt/apache.crt"`` and ``SSL_PEM_FILE="/etc/apache2/ssl/key/apache.key"``

## PCA
The Partition-Control-Agent(PCA) manages a Partition of detectors and global subsystems.
Basic settings for the PCA can be found in the ``init.cfg`` file.
The PCA manages a global state for the partition in a finite state machine. The graph for the FSM is generated over the ``PCAStatemachine.csv`` file. Changes to the PCA state machine will most likely require in the ``states.py`` file which (for the most part) assigns the states of the csv to python variables. Furthermore a state machine change will likely require changes to the function ``checkGlobalState`` (probably more) in ``PCA.py``.

The PCA client can be startet by typing ``./PCA.py <pca id>`` while replacing ``<pca id>`` with the actual partition name.

The PCA client can also be started automatically by the ECA be setting ``START_CLIENTS = True`` in the ``Django/ECS_GUI/ECS_GUI/settings.py`` file. The ECA then tries to start the clients over ssh. It is required that the user who runs the ECA can login into the target node without entering his/her password.

## Client Agents
Client Agents are the interface to the actual experiment subssytems like detectors. There is a separation between  detectors which belong to exactly one partition and global systems like the DCS which belong to all partitions.
Basic settings for subsystem controller can be found in ``subsystem.cfg``.
Each subsystem has it's own state machine. These FSMs are again provided in csv files. Again changes to these will most likely also require changes to ``states.py`` and ``DetectorController.py`` or ``GlobalSystemClient.py``.

Detectors can be startet by ``./DetectorController.py -s <startState> -c <startConfig> <detector Id>``.

Global Systems can be startet by ``./GlobalSystem.py -s <startState> -o <startConfig> <System (TFC,DCS,QA,FLES or all)>``

The options startState and startConfig should only be used for debugging the controller should get the actual state from the subsystem itself .

## starting scripts
There are several start scripts for the ECS.
* The script ``demo.sh`` starts a set demo detector dummys.
* ``start.sh <number of detectors>`` starts n detectors with ascending numeric ids.
* ``startSlurm.sh <number of detectors>`` starts n detectors,a test PCA and Global Systems in an SLURM cluster. It executes the scripts ``pca_and_globalSystem_start.sh`` and ``detectorStart.sh`` on the SLURM nodes

The python script ``Django/ECS_GUI/createTestPCA.py`` can be used to quickly create a test partition with a large number of detectors, which run distributed in a SLURM cluster.

#The user interface
# Setup
Only one user at a time should be able to control a partition or create and edit partitions. To achieve this, the user right management from the django framework is used.

To use the interface every user needs an account. User creation is done through the standard django admin page. To use this one first needs to create a superuser by using the following command in the project folder:
``python3 Django/ECS_GUI/manage.py createsuperuser``
and follow the prompted instructions.

**Do not use the superuser as a regular user!(only to create and edit users)**

To create user access the admin page by using the following link: ``https://<serveraddress:port>/admin/``.

Click on adduser, type in a name and password and click on continue editing. At user Permissions add
* GUI | pcamodel | can take control: to allow the user to take control of partitions
* GUI | ecsmodel | can take control: to allow the user to take control of the ecs

# Index Page
![index screenshot picture](https://raw.githubusercontent.com/dbruins/ECS/master/Django/ECS_GUI/Screenshots/index.png "screenshot of index page")
After the login the user is redirected to the index page. At the index page all partitions and subsystem states are listed.

With a click on the icon in the right left corner one can logout or take control of a partition or the ecs. If another user is currently in control of a partition or the ecs it will be displayed there.

The left frame shows the navigation bar. Only elements for which the user has the appropriate rights will be shown.

At the buttom all log messages from PCAs and ECS are shown
# PCA Page
![PCA screenshot picture](https://raw.githubusercontent.com/dbruins/ECS/master/Django/ECS_GUI/Screenshots/pca.png "screenshot of PCA page")
At the pca page user can see the current status of the partition and control (if he or her has the rights to do so).

The control buttons at the bottom are only shown if the user has the control right. Buttons for actions which are unavailable in the current PCA state are deactivated.

Before the configuration the user must select a configuration. With a click on the "select configuration" button a new control window will be displayed where the user can select configurations for the subsystems. A set of subsystem configurations can be saved a new global tag for the PCA. When the user clicks on the configure button the PCA will configure the next subsystem in the global state machine hierarchie. With the "auto configure" option selected the PCA will automatically continue configuring the partitions subsystems until it is ready for data taking.

The abort button resets the whole partition the unconfigured state.

the start and stop button start and end the data taking. A data taking can only be started if all subsystems are configured.

In the logbox only log messages from the current PCA are shown.

# Detector Activities
The possible detector actions on ECS level are adding,deleting and moving between partitions. The pages for the actions can be selected from the navigation bar if the user has the ECS right.

For creating a new detector the user has to enter the needed information for the controller agent. This includes an id for the detector, address and ports.

For moving a detector the user has to select a from and to partition. From the from partition several detectors for movement to the to partition can be selected.

If an error occurs during the detector movement the state before the movement will be restored. The user can select to ignore the error and force the moving, which should only be done with great caution. The force moving option can for example be used if a controller agent can't be reached, the informing of the agent will then be skipped. Detectors can only be moved, if both partitions are in a non configuring state. Detectors can also be assigned to no partition by selecting the partition "unmapped detectors".

# Partition Activities
Action pages for partitions are deleting and adding of partitions. Both actions work basically the same as for detectors. Like with detector actions the user also needs the ECS control to perform these actions.

# Configuration Activities
![config edit screenshot picture](https://raw.githubusercontent.com/dbruins/ECS/master/Django/ECS_GUI/Screenshots/conf.png "screenshot of config edit page")
Configuration actions include creating/editing/deleting new subsystem configuration and global configuration tags.

The user can create new configurations for subsystems. An old configurations can be selected for edit or an entirely new one can be created. A subsystem configuration tag includes a set parameters with assigned values.

A set of Subsytem configuration can be assigned to a global tag. These global tags can also be created from scratch or edited from an existing tag.
