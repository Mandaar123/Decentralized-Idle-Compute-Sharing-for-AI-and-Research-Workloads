const axios = require("axios");

const SERVER = "http://localhost:8000";

const code = `
# DEMO TASK — MNIST CNN Training

import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import datasets, transforms
from torch.utils.data import DataLoader

print("=" * 50)
print("Distributed Compute Task: MNIST CNN Training")
print("=" * 50)

# Model
class SimpleCNN(nn.Module):
    def __init__(self):
        super().__init__()
        self.conv = nn.Sequential(
            nn.Conv2d(1, 16, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2),
            nn.Conv2d(16, 32, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2),
        )
        self.fc = nn.Sequential(
            nn.Linear(32 * 7 * 7, 128),
            nn.ReLU(),
            nn.Linear(128, 10),
        )

    def forward(self, x):
        return self.fc(self.conv(x).view(x.size(0), -1))

# Data
transform = transforms.Compose([
    transforms.ToTensor(),
    transforms.Normalize((0.1307,), (0.3081,))
])

train_data = datasets.MNIST('/tmp/data', train=True, download=True, transform=transform)
test_data  = datasets.MNIST('/tmp/data', train=False, download=True, transform=transform)

train_loader = DataLoader(train_data, batch_size=128, shuffle=True)
test_loader  = DataLoader(test_data, batch_size=256, shuffle=False)

# Training setup
model = SimpleCNN()
optimizer = optim.Adam(model.parameters(), lr=0.001)
criterion = nn.CrossEntropyLoss()

print("\\nTraining for 3 epochs...")

for epoch in range(1, 4):
    model.train()
    total_loss = 0

    for batch_idx, (data, target) in enumerate(train_loader):
        optimizer.zero_grad()
        loss = criterion(model(data), target)
        loss.backward()
        optimizer.step()
        total_loss += loss.item()

        if batch_idx % 100 == 0:
            print(f"Epoch {epoch} | Batch {batch_idx} | Loss: {loss.item():.4f}")

    # Evaluation (INSIDE loop ✅)
    model.eval()
    correct = 0

    with torch.no_grad():
        for data, target in test_loader:
            correct += model(data).argmax(1).eq(target).sum().item()

    acc = 100. * correct / len(test_data)
    print(f"Epoch {epoch} complete — Accuracy: {acc:.2f}%")

print("=" * 50)
print(f"Final Accuracy: {acc:.2f}%")
print("Training complete on worker 🚀")
print("=" * 50)
`;

async function submitTask() {
  try {
    const res = await axios.post(`${SERVER}/tasks`, {
      task_type: "python",
      code: code
    });

    console.log("✅ Task submitted:", res.data);
  } catch (err) {
    console.error("❌ Error submitting task:", err.message);
  }
}

submitTask();