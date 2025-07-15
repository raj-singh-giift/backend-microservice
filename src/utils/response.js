const successResponse = (isSuccessFull = true, returnObject = {}, exceptionMessage = '', errorCode = 0) => {
    return {
        isSuccessFull: isSuccessFull,
        returnObject: returnObject,
        exceptionMessage: exceptionMessage,
        errorCode: errorCode
    };
};

const errorResponse = (isSuccessFull = false, returnObject = {}, exceptionMessage = '', errorCode = 0) => {    
    return {
        isSuccessFull: isSuccessFull,
        returnObject: returnObject,
        exceptionMessage: exceptionMessage,
        errorCode: errorCode
    };
};

export { successResponse, errorResponse };