> *This project has been created as part of the 42 curriculum by rhssayn.*

# 🌐 Code Nexus  
### *Polymorphic Data Streams in the Digital Matrix*

---

## 📌 Description

**Code Nexus** is an advanced Python project focused on **object-oriented design,
polymorphism, and scalable data stream engineering**.

This project explores how complex data systems can be built using **abstract base
classes**, **method overriding**, and **polymorphic interfaces**. You will design
processing architectures where multiple data types are handled through a **single,
consistent interface**, while still exhibiting specialized behavior.

The core philosophy of Code Nexus is simple:

> **Same interface. Different behavior.**

This is the foundation of professional, maintainable, and extensible software systems.

---

## 🎯 Project Objectives

By completing this project, you will learn how to:

- 🧠 Design clean inheritance hierarchies
- 🔁 Use polymorphism to process different data types uniformly
- 🧱 Build abstract base classes with `ABC` and `@abstractmethod`
- 🧬 Apply method overriding correctly and purposefully
- 🧩 Combine duck typing and classical inheritance
- 🛡 Protect data streams using structured exception handling
- 🏗 Architect scalable, enterprise-style data pipelines

---

## 🧪 Exercises Overview

### 🧱 Exercise 0 — Data Processor Foundation
Build the core processing architecture.

You will create:
- An abstract `DataProcessor` base class
- Specialized processors for numeric, text, and log data
- A shared interface with different internal behaviors

This exercise establishes the foundation of **polymorphic design**.

---

### 🌊 Exercise 1 — Polymorphic Streams
Design adaptive data streams capable of processing mixed data types.

You will implement:
- A `DataStream` abstract base class
- Specialized stream types (sensor, transaction, event)
- A stream processor that works without knowing concrete implementations

This exercise demonstrates **subtype polymorphism in real data systems**.

---

### 🧬 Exercise 2 — Nexus Integration
Integrate everything into a complete enterprise-style pipeline.

You will build:
- A configurable processing pipeline
- Multiple processing stages using protocols (duck typing)
- Specialized adapters for different data formats
- A manager that orchestrates multiple pipelines polymorphically

This is the final system that combines **inheritance, overriding, composition,
and error recovery**.

---

## ⚙️ Engineering Rules & Constraints

- Python **3.10+**
- Code must follow **flake8** standards
- **Only standard library imports** are allowed
- Full type annotations using `typing`
- All function parameters and return types must be annotated
- Abstract base classes must use `ABC` and `@abstractmethod`
- Overridden methods must keep the **same signature**
- Programs must **never corrupt data streams**
- Exception handling must be explicit and intentional

---

## 🧠 Design Principles

This project enforces professional software engineering principles:

- **Interface consistency**  
  Subclasses must respect parent method signatures.

- **Behavioral specialization**  
  Each subclass must provide meaningful, distinct behavior.

- **Polymorphic usage**  
  Objects should be usable interchangeably through shared interfaces.

- **Separation of concerns**  
  Processing stages, pipelines, and orchestration logic must remain independent.

This approach mirrors real-world data platforms and stream processing systems.

---

## 🧪 Testing & Execution

Each exercise can be tested directly:

```bash
python3 your_file.py
```

## 👤 Author

**Redouane Hssayn (Finn)/(rhssayn)**
Student at **1337 - 42 Network**

If this project helps you, feel free to ⭐ the repository on GitHub!