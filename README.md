# Dropwizard Service Discovery [![Travis build status](https://travis-ci.org/santanusinha/dropwizard-service-discovery.svg?branch=master)](https://travis-ci.org/santanusinha/dropwizard-service-discovery)

Provides service discovery to dropwizard services. It uses [Ranger](https://github.com/flipkart-incubator/ranger) for service discovery.

## Dependency for bundle
```
<dependency>
    <groupId>io.appform.dropwizard.discovery</groupId>
    <artifactId>dropwizard-service-discovery-bundle</artifactId>
    <version>2.0.23-1</version>
</dependency>
```

## Dependency for client
```
<dependency>
    <groupId>io.appform.dropwizard.discovery</groupId>
    <artifactId>dropwizard-service-discovery-client</artifactId>
    <version>2.0.23-1</version>
</dependency>
```

## How to use the bundle

You need to add an instance of type _ServiceDiscoveryConfiguration_ to your Dropwizard configuration file as follows:

```
public class AppConfiguration extends Configuration {
    //Your normal config
    @NotNull
    @Valid
    private ServiceDiscoveryConfiguration discovery = new ServiceDiscoveryConfiguration();
    
    //Whatever...
    
    public ServiceDiscoveryConfiguration getDiscovery() {
        return discovery;
    }
}
```

Next, you need to use this configuration in the Application while registering the bundle.

```
public class App extends Application<AppConfig> {
    private ServiceDiscoveryBundle<AppConfig> bundle;
    @Override
    public void initialize(Bootstrap<AppConfig> bootstrap) {
        bundle = new ServiceDiscoveryBundle<AppConfig>() {
            @Override
            protected ServiceDiscoveryConfiguration getRangerConfiguration(AppConfig appConfig) {
                return appConfig.getDiscovery();
            }

            @Override
            protected String getServiceName(AppConfig appConfig) {
                //Read from some config or hardcode your service name
                //This wi;l be used by clients to lookup instances for the service
                return "some-service";
            }

            @Override
            protected int getPort(AppConfig appConfig) {
                return 8080; //Parse config or hardcode
            }
        };
        
        bootstrap.addBundle(bundle);
    }

    @Override
    public void run(AppConfig configuration, Environment environment) throws Exception {
        ....
        //Register health checks
        bundle.registerHealthcheck(() -> {
                    //Check whatever
                    return HealthcheckStatus.healthy;
                });
        ...
    }
}
```
That's it .. your service will register to zookeeper when it starts up.

Sample config section might look like:
```
server:
  ...
  
discovery:
  namespace: mycompany
  environment: production
  zookeeper: "zk-server1.mycompany.net:2181,zk-server2.mycompany.net:2181"
  ...
  
...
```

The bundle also adds a jersey resource that lets you inspect the available instances.
Use GET /instances to see all instances that have been registered to your service.

## How to use the client
The client needs to be created and started. Once started it should never be stopped before the using service
itself dies or no queries will ever be made to ZK. Creation of the client is expensive.

```
ServiceDiscoveryClient client = ServiceDiscoveryClient.fromConnectionString()
                .connectionString("zk-server1.mycompany.net:2181, zk-server2.mycompany.net:2181")
                .namespace("mycompany")
                .serviceName("some-service")
                .environment("production")
                .objectMapper(new ObjectMapper())
                .build();
```

Start the client
```
client.start();
```

Get a valid node from the client:
```
Optional<ServiceNode<ShardInfo>> node = client.getNode();

//Always check if node is present for better stability
if(node.isPresent()) {
    //Use the endpoint details (host, port etc) 
    final String host = node.get().getHost();
    final int port = node.get().getPort();
    //Make service calls to the node etc etc
}
```

Close the client when not required:
```
client.stop();
```

*Note*
- Never save a node. The node query is extremely fast and does not make any remote calls.
- Repeat the above three times and follow it religiously.

## License
Apache 2

# NOTE
Package and group id has changed from `io.dropwizard.discovery` to `io.appfrom.dropwizard.discovery` from 1.3.12-2.
