const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const PROTO_PATH = __dirname + '/user.proto';
const packageDefinition = protoLoader.loadSync(PROTO_PATH);
const userService = grpc.loadPackageDefinition(packageDefinition).UserService;
const client = new userService('localhost:50051',
    grpc.credentials.createInsecure());
function getUser(userId) {
    client.GetUser({ user_id: userId }, (error, response) => {
        if (error) {
            console.error("Error:", error);
        } else {
            console.log("User:", response.user);
        }
    });
}
getUser('123');
//getUser('456');