# Python-RabbitMQ

## How to install local environment
- Install docker
- Run rabbitmq container
```
docker run -it --name rabbitmq --restart always -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

## Check the rabbit server
```
http://localhost:15672/
user: guest
pass: guest
```