from mrjob.job import MRJob
import nltk
import re

WORD_RE = re.compile(r"[\w']+")

class smAnalysis(MRJob):


	def init(self):
		self.poslisting=['happy','care','win','love','empathy','glow','fantastic','handsome', 'clever', 'rich']
		self.neglisting=['no','die','murder','kill','cry']
		self.map={}
		self.map.setdefault('pos',0)
		self.map.setdefault('neg',0)

	def smainitialmapper(self,_,line):
		for word in WORD_RE.findall(line):
			if word in self.poslisting:
				self.map['pos']=self.map['pos']+1
			if word in self.neglisting:
				self.map['neg']=self.map['neg']+1
	
	def smafinalmapper(self):
		for word,val in self.map.iteritems():
			yield word,val
		

	def smacombiner(self ,word,countpair):
		yield word,sum(countpair)

	def smareducer(self,word,count):
		yield word,sum(count)

	
	def steps(self):
		return [self.mr(mapper_init=self.init,
                        mapper=self.smainitialmapper,
                        mapper_final=self.smafinalmapper,
                        combiner=self.smacombiner,
                        reducer=self.smareducer)]


if __name__ == '__main__':
    smAnalysis.run()

			







