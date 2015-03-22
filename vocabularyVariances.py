from mrjob.job import MRJob
import nltk
from nltk.corpus import stopwords
import re
from string import punctuation
#.strip(punctuation)


WORD_RE = re.compile(r"[\w']+")

class smAnalysis(MRJob):


	def init(self):
		
		self.fileid=stopwords.words('english')
		self.map={}
		self.map.setdefault('content',0)
		self.map.setdefault('text',0)
		
	def smamapper(self,_,line):
		for word in WORD_RE.findall(line):
			
			self.map['text']=self.map['text']+1

			if word.lower() not in self.fileid:
				self.map['content']=self.map['content']+1
		
	def smafinalmapper(self):
		for key,value in self.map.iteritems():
			yield key,value
			

	def smacombiner(self,key,value):
		yield key,sum(value)		


	
	def smareducer(self,word,count):
		yield word,sum(count)

	
	def steps(self):
		return [self.mr(mapper_init=self.init,
                        mapper=self.smamapper,
						mapper_final=self.smafinalmapper,
                        combiner=self.smacombiner,
                        reducer=self.smareducer)]
                        

if __name__ == '__main__':
    smAnalysis.run()

			







