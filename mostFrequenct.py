from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class mostfrequent(MRJob):

	def init(self):
		self.map={}
		
		

	def mfmapper(self,_,line):
		for word in WORD_RE.findall(line):
			word=word.lower()
			self.map.setdefault(word,0)
			self.map[word]=self.map[word]+1
	

	def mapperfinal(self):
		for word,val in self.map.iteritems():
			yield word,val

	def mfeducer(self,word,count):
		yield word,sum(count)


	def mfeducerglobal(self,word,count):
		yield word,sum(count)

	def steps(self):
		return [self.mr(mapper_init=self.init,
                        mapper=self.mfmapper,
                        mapper_final=self.mapperfinal,
                        combiner=self.mfeducer,
                        reducer=self.mfeducerglobal)]


if __name__ == '__main__':
    mostfrequent.run()

		
		
			
