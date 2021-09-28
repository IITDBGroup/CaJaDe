import subprocess
from time import sleep

def run_command(command):
    p = subprocess.Popen(command,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=True)
    # This ensures the process has completed, AND sets the 'returncode' attr
    while p.poll() is None:
      # print('we are here!')
      sleep(.1) # sleep until finished
    # Read buffers
    err = p.stderr.read()
    std = p.stdout.read()
    errcode = p.returncode
    return err, std, errcode

################################################################################
# Wrapper around the gprom commandline client for easy python access
class GProMWrapper:
   'Wrapper around the gprom commandline'

   # stores connection parameters and other gprom options
   def __init__(self, user, passwd, host, port='1521', db='orcl', frontend='', backend='oracle', plugins={ 'executor' : 'sql' }, options={}):
        self.user = user
        self.passwd = passwd
        self.host = host
        self.port = port
        self.db = db
        self.frontend = frontend
        self.backend = backend
        self.plugins = plugins
        self.options = options
   def constructCommand(self,query,loglevel=0,plugins={},frontend='',options='', ec_options=False):
        if(ec_options):
            loglevel=1
            ec_options_cmd = ["-Oselection_move_around TRUE -Oremove_unnecessary_columns FALSE -Opush_selections FALSE -Omerge_ops FALSE ",
               "-Ofactor_attrs FALSE -Omaterialize_unsafe_proj FALSE -Oremove_redundant_projections FALSE ",
               "-Oremove_redundant_duplicate_removals FALSE -Oremove_redundant_window_operators FALSE -Opullup_duplicate_removals FALSE ",
               "-Opullup_prov_projections FALSE -Opush_selections_through_joins FALSE -heuristic_opt TRUE "]
        
        gprom_cmd=['gprom','-loglevel',loglevel,'-backend',self.backend]

       # set frontend
        if (frontend != ''):
          gprom_cmd+=['-frontend',frontend]
        # set connection options
        gprom_cmd+=['-user',self.user,'-passwd',self.passwd,'-host',self.host,'-port',self.port,'-db',self.db]
        # setup plugins
        for key, value in self.plugins.items():
           if key in plugins:
               gprom_cmd+=['-P'+key, plugins[key]]
           else:
               gprom_cmd+=['-P'+key, value]
        # boolean options
        if len(options) > 0:
           gprom_cmd+=options
        if(ec_options):
           gprom_cmd+=ec_options_cmd 
        # pass quoted query
        quotedQuery='"' + query + '"'
        gprom_cmd+=['-query', quotedQuery]
        # create one string
        gprom_cmd=' '.join(map(str,gprom_cmd))
        print(gprom_cmd)
        return gprom_cmd

   def executeAndCollectErrors(self,query,errorloglevel=3,mode='sql',frontend='',inputdb='', ec_options=False):
       runPlugins={'executor':mode}
       runFrontend=frontend
       runOptions=inputdb
       orig_cmd=self.constructCommand(query,plugins=runPlugins,frontend=runFrontend,options=runOptions, ec_options=ec_options)
       # print(orig_cmd)
       err, std, errcode = run_command(orig_cmd)
       # if errcode != 0:
       #     debug_cmd=self.constructCommand(query,errorloglevel,plugins=runPlugins,frontend=runFrontend,options=runOptions, ec_options=ec_options)
       #     err, std, errcode = run_command(debug_cmd)
       #     return errcode, std + '\n' + err
       return 0, std
   
   def createDotFile (self,query,dotfile):
       errcode, output = self.executeAndCollectErrors(query,mode='gp',frontend='dl',inputdb='')
       if errcode != 0:
           return errcode, output
       writer = open(dotfile, 'w')
       writer.write(output)
       writer.close()
       return 0, ''

   def runGraphviz (self,dotfile,imagepath):
       dot_cmd=['dot','-Tsvg','-o',imagepath,dotfile]
       err, std, errcode = run_command(' '.join(dot_cmd))
       return errcode, err
   
   def generateProvGraph (self,query,imagepath,dotfile):
       gprom_return, gprom_log = self.createDotFile(query,dotfile)
       if gprom_return == 0:
           dot_return, dot_log = self.runGraphviz(dotfile,imagepath)
       else:
           dot_return, dot_log = 0, ''
       return (gprom_return + dot_return), gprom_log, dot_log

   def runDLQuery (self,query,mode='sql',frontend='dl'):
       errcode, output = self.executeAndCollectErrors(query,mode=mode,frontend=frontend)
       return errcode, output

   def runInputDB (self,query,mode='sql',frontend='dl',inputdb='-inputdb'):
       errcode, output = self.executeAndCollectErrors(query,mode=mode,frontend=frontend,inputdb=inputdb)
       return errcode, output

   def runQuery (self,query,mode='sql',frontend='', ec_options=False):
       errcode, output = self.executeAndCollectErrors(query,mode=mode,frontend=frontend, ec_options=ec_options)
       # print("runQuery: " + str(output))
       return errcode, output

   def printHelp (self):
       err, std, errcode = run_command("gprom -help")
       print("GProM Commandline Python Wrapper give Datalog query as first arg or just call gprom directly which supports the following args\n" + '-' * 80 + "\n" + std + err)

