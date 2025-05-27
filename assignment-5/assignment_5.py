import time
import os
import utilities
from db_connection import get_mariadb_connection, get_postgres_connection


    


def main():
    
    # Setup 1     Joint Strategy: Hash Join                        Index Setup: No Index
    
    # Query 1
    """ SELECT name , title
    """  """FROM Auth , Publ
    """  """WHERE Auth . pubID = Publ . pubID ;
    """
    
    
    # Query 2 
    """ SELECT title
    FROM Auth , Publ
    WHERE Auth . pubID = Publ . pubID AND Auth . name = ’ Divesh Srivastava ’ """
    
    
    
    
    # Setup 2     Joint Strategy: Sort-Merge Join                  Index Setup: No Index
    
    # Query 1
    """ SELECT name , title
    """  """FROM Auth , Publ
    """  """WHERE Auth . pubID = Publ . pubID ;
    """
    
    
    # Query 2 
    """ SELECT title
    FROM Auth , Publ
    WHERE Auth . pubID = Publ . pubID AND Auth . name = ’ Divesh Srivastava ’ """
    
    # Setup 3     Joint Strategy: Sort-Merge Join                  Index Setup: No Index
    
    # Query 1
    """ SELECT name , title
    """  """FROM Auth , Publ
    """  """WHERE Auth . pubID = Publ . pubID ;
    """
    
    
    # Query 2 
    """ SELECT title
    FROM Auth , Publ
    WHERE Auth . pubID = Publ . pubID AND Auth . name = ’ Divesh Srivastava ’ """
    
    # Setup 4     Joint Strategy: Sort-Merge Join                  Index Setup: No Index
    
    # Query 1
    """ SELECT name , title
    """  """FROM Auth , Publ
    """  """WHERE Auth . pubID = Publ . pubID ;
    """
    
    
    # Query 2 
    """ SELECT title
    FROM Auth , Publ
    WHERE Auth . pubID = Publ . pubID AND Auth . name = ’ Divesh Srivastava ’ """
    
    # Setup 5     Joint Strategy: Nested Loop Join                 Index Setup: No Index
    
    # Query 1
    """ SELECT name , title
    """  """FROM Auth , Publ
    """  """WHERE Auth . pubID = Publ . pubID ;
    """
    
    
    # Query 2 
    """ SELECT title
    FROM Auth , Publ
    WHERE Auth . pubID = Publ . pubID AND Auth . name = ’ Divesh Srivastava ’ """
    
    # Setup 6     Joint Strategy: Nested Loop Join                 Index Setup: No Index
    
    # Query 1
    """ SELECT name , title
    """  """FROM Auth , Publ
    """  """WHERE Auth . pubID = Publ . pubID ;
    """
    
    
    # Query 2 
    """ SELECT title
    FROM Auth , Publ
    WHERE Auth . pubID = Publ . pubID AND Auth . name = ’ Divesh Srivastava ’ """
    
    # Setup 7     Joint Strategy: Nested Loop Join                 Index Setup: No Index
    
    # Query 1
    """ SELECT name , title
    """  """FROM Auth , Publ
    """  """WHERE Auth . pubID = Publ . pubID ;
    """
    
    
    # Query 2 
    """ SELECT title
    FROM Auth , Publ
    WHERE Auth . pubID = Publ . pubID AND Auth . name = ’ Divesh Srivastava ’ """
    
    

if __name__ == "__main__":
    main()








