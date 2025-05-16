# fine-tune a sentence transformer model for semantic search
from torch.utils.data import Dataset
from sentence_transformers import losses, InputExample, SentenceTransformer
from torch.utils.data import DataLoader
from CM.utils import *
import re
from collections import Counter

driver = getDriver("archamap")
data = getQuery(
    "match (n:CERAMIC) where size(n.names) > 1 unwind n.names as name return distinct n.CMID as CMID, name", driver, type="df")


def tokenize(name):
    # Lowercase, remove punctuation except hyphen, and split into words
    name = re.sub(r'[^\w\s-]', '', name.lower())
    return name.split()


all_tokens = [token for name in data['name'].dropna()
              for token in tokenize(name)]
token_counts = Counter(all_tokens)
common_words = token_counts.most_common(100)
common_words

nouns = ["cibola", "tusayan", "verde", "rio", "chuska", "san", "mogollon", "gila", "johns", "reserve", "mimbres", "johns", "puerco", "colorado", "tularosa", "tsegi", "salado", "sacaton", "santa", "pueblo", "pinedale", "juan", "mancos",
         "kayenta", "jeddito", "wingate", "zuni", "alma", "gallup", "creek", "tanque", "cruz", "deadmans'", "walnut", "holbrook", "el", "paso", "tonto", "rincon", "chaco", "showlow", "mcelmo", "carlos", "kanaa", "mcdonald", "butte", "chihuahuan"]
filtered_stop_words = [word for word, _ in common_words if word not in nouns]


def clean_name(name, stop_words):
    tokens = re.sub(r"[^\w\s-]", "", name.lower()).split()
    return " ".join([t for t in tokens if t not in stop_words])


data['clean_name'] = data['name'].dropna().apply(
    lambda x: clean_name(x, filtered_stop_words))

train_examples = [
    InputExample(texts=[row['clean_name'], row['name']], label=1.0)
    for _, row in data[['clean_name', 'name']].dropna().iterrows()
]


class SentencePairDataset(Dataset):
    def __init__(self, examples):
        self.examples = examples

    def __getitem__(self, idx):
        return self.examples[idx]

    def __len__(self):
        return len(self.examples)


train_dataset = SentencePairDataset(train_examples)

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

train_dataloader = DataLoader(
    train_examples, shuffle=True, batch_size=16, pin_memory=False, num_workers=0)

train_loss = losses.CosineSimilarityLoss(model)

model.fit(
    train_objectives=[(train_dataloader, train_loss)],
    epochs=4,
    warmup_steps=100,
    output_path="models/fine-tuned-search-model"
)

model = SentenceTransformer('models/ceramics/fine-tuned-search-model')
