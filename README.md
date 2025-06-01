# FiftyOne Snapshots Plugin

A FiftyOne plugin for creating, exporting, and importing snapshots of samples in your datasets.

## Features

- **Snapshot Samples**: Export selected samples, the current view, or an entire dataset along with their metadata for backup or transfer.
- **Import Snapshots**: Import previously exported snapshots into the current dataset or a new dataset.
- **Flexible Data Management**: Choose where to store media files and organize your samples with tags.

## Installation

### From FiftyOne Plugin Manager

```bash
fiftyone plugins download https://github.com/yourusername/fiftyone-snapshots-plugin
```

### Manual Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/fiftyone-snapshots-plugin.git
```

2. Install the plugin:
```bash
cd fiftyone-snapshots-plugin
fiftyone plugins upload .
```

## Usage

### Snapshot Samples

1. Open your FiftyOne dataset
2. Select the samples you want to snapshot (optional)
3. Open the plugin panel and select "Snapshot Samples"
4. Choose your target (selected samples, current view, or entire dataset)
5. Select an output directory
6. Click "Run"

### Import Snapshots

1. Open FiftyOne
2. Open the plugin panel and select "Import Snapshots"
3. Choose the directory containing previously exported snapshots
4. Select whether to import into the current dataset or another dataset
5. Add tags (optional)
6. Click "Run"

## Development

### Prerequisites

- FiftyOne 0.22.2 or later
- Python 3.6 or later

### Local Development

1. Clone this repository
2. Navigate to the repository directory
3. Install development dependencies:
```bash
pip install -e ".[dev]"
```

4. Run the development server:
```bash
fiftyone app run -p
```

## License

[MIT License](LICENSE)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request
