
[![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)

[![forthebadge](https://forthebadge.com/images/badges/gluten-free.svg)](https://forthebadge.com)

[![forthebadge](https://forthebadge.com/images/badges/powered-by-coffee.svg)](https://forthebadge.com)

### Setting up the database

### Dabase setup is necessary to run this project as this project requires a MongoDb server to be up and running in the local system. Setting up Mongo using docker is convinient and highly recommended.

Following are the steps to setup database using docker:

1. Pull the official mongo docker image from docker hub using ````docker pull mongo````
2. Make sure that any old mongo container is removed if not then stop any running container using ````docker stop <container_id>```` container_id can be fetched using ````docker ps````. Remove the stopped container using ````docker rm <container_id>````
3. Now run ````docker run -d -p 27017:27017 --name mongo_container mongo```` to start the docker container on port 27017.

###### Database can also be setup in one go using database_setup.sh file provided. Just make sure that any previous container is stopped and removed then run ````sudo bash setup_database.sh````.

### Docker run command

docker run -d -p 27017:27017 --name mongo_container mongo
#### Steps to run the project
1. It is advisable to create and activate a virtualenv before starting the project. However it is not a requirement to run the project.
2. The application can be started by running a single bash script run.sh inside src folder once database setup is done.
3. Make sure that you current directory is src and then run ````bash run.sh host_name <number_of_peers>````
4. Hostname is a mandatory parameter and needs to be passed as a command line argument for the program to start.
5. Number of peers is optional paramter if passed the network will generate a network with equivalent number of peers otherwise it will default to six peer network based on the default config provided in config_file.
6. A sample run command might look like ````bash run.sh localhost 10```` or ```` bash run.sh localhost````
7. In first case a 10 peer network will be created whereas in the second a network of 6 peers will be created.
8. The output of the program after running the network for a few minutes is present in ````src/tmp/output/output.txt````

##### A know issue

We have used matplotlib to show a graphical representation of the network. Plotting of graph might not work iff a GUI backend is not present. This issue can be resolved in two ways.
1. pass **show_bazar** argument as false when calling **create_bazar** function from main. This will prevent graph_generator to be invoked and hence resolving the error. However in this way the graphical representation of the network won't pe drawn.
2. Another way is to install a GUI-backend for matplotlib ````python
sudo apt-get install python3-tk````

#### Documentation links


1  ![design_doc](https://github.com/alokrkmv/lab-2-the_trading_post/blob/main/src/Documentation/the_trading_post_doumentation.pdf)
2. ![problem statement](https://github.com/alokrkmv/lab-2-the_trading_post/blob/main/src/Documentation/Lab%202_problem_statement.pdf)

    
  
