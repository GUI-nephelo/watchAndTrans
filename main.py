import watch
import uvicorn
from trans import app
    
if __name__=="__main__":
    ob = watch.main()
    uvicorn.run(app, host="localhost", port=8000)
    print("Observer stop")
    ob.stop()
