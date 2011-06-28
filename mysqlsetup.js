var mysql = require('./mysql.js').mysql;

function MysqlSetup(config){
	//TODO config tests
	this.config = config;
}

MysqlSetup.prototype = {
	config:null,
	clients:[],
	jobs:0,
	start:function (done){
		var z = this
		,config = this.config;
		
		for(var i in config){
			(function(){
				var client = new mysql.Client()
				,c = config[i]
				,next = function(error){
					z.jobs--;
					if(z.jobs == 0) {
						if(c.complete){
							c.complete(error,client,function(){
								z.closeClients();
								done(error,client);
							});
						} else {
							z.closeClients();
							done(error,client);
						}
					}
				};
				
				client.user = c.db_user;
				client.password = c.db_pass;
				client.host = c.db_host;
				client.connect();
				
				z.clients.push(client);
				z.jobs++;
				z.job(client,c,next);
			}());
		}
	},
	job:function(client,config,done){
		var z = this;
		this.haveDatabase(client,config.db_name,function(error,exists){
			if(error) done(error,false);
				  
			if(exists){
				client.query('use '+config.db_name,function(error,result,fields){
					if(error) done(error,false);
					z.ensureTables(client,config.tables,done);
				});
			} else {
				client.query("create database "+config.db_name,function(error,result,fields){
					client.query('use '+config.db_name,function(error,result,fields){
						if(error) done(error,false);
						z.ensureTables(client,config.tables,done);
					});
				})
			}
		});
	},
	closeClients:function(){
		while(this.clients.length){
			this.clients.shift().end();
		}
	},
	haveDatabase:function(client,db,done){
		client.query("show databases",function(err,result,fields){
			var exists = false;
					
			for(var i=0,j=result.length;i<j;i++){
				if(result[i].Database == db){
					exists = true;
					break;
				}
			}
			done(err,exists);
		});
	},
	ensureTables:function(client,config,next){
		var c = {};
		config.forEach(function(v,k){
			c[v.name] = v;
		});
		client.query('show tables',function(err,result){
			for(var i=0,j=result.length;i<j;i++){

				if(c[result[i].Table]){
					delete c[result[i].Table];
				}
			}
			var toMake = Object.keys(c)
			,todo = toMake.length
			,errors = [];
			if(todo){
				while(toMake.length){
					client.query(c[toMake.shift()].schema,function(error,result){
						if(error) {
							errors.push(error);
						}
						todo--;
						if(!todo) {
							next(errors.length?errors:false,true);
						}
					});
				}
			} else {
				next(false,true);
			}
		});
	}
};

module.exports = {
	MysqlSetup:MysqlSetup
};