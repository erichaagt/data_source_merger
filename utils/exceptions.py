class MergerError (Exception):
    
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message 
        
    def get_message (self):
        return self.message    
