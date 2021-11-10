# data_manipulation
Data cleaning, processing, upload and event management

This project receives sensor data from a rabbitmq server, processes this data, saves it in an s3 bucket and shoots out other events to further process the data or
alert users of errors.

This project plays around with dependency injection, dependency inversion, observer pattern, factory pattern, usage of inheritance(abstract classes) and relies heavily on composition instead.
I have also used the observer pattern for handling some events.
