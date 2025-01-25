


class AWSCredentialError(Exception):

    def __init__(self,message):
        self.message = message
        super().__init__(self.message)

    def get_message(self)  -> str:
        return self.message




class AWSConnectionError(Exception):

    def __init__(self,message):
        self.message = message
        super().__init__(self.message)


    def get_message(self):
        return self.message


class AWSBucketCreationException(Exception):

    def __init__(self,message):
        self.message = message
        super().__init__(self.message)


    def get_message(self) -> str:
        return self.message