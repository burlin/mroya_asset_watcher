# Mroya Asset Watcher

Monitors ftrack assets for updates and notifies when new versions are available.

## Features

- **Watch assets from DCC**: Subscribe to updates for specific assets/components
- **Auto-detect updates**: Listen for new AssetVersion events in real-time
- **Location tracking**: Monitor when components appear on target locations
- **Auto-transfer**: Automatically trigger transfers for new versions
- **Missed update detection**: Check for updates that happened while offline
- **Connect UI**: Tab in ftrack Connect showing watched assets and status

## Installation

1. Copy `mroya_asset_watcher` folder to your ftrack plugins directory
2. Add path to `FTRACK_CONNECT_PLUGIN_PATH`
3. Restart ftrack Connect

## Usage

### From DCC (Houdini/Maya)

```python
import ftrack_api

session = ftrack_api.Session(auto_connect_event_hub=True)

# Start watching an asset
session.event_hub.publish(
    ftrack_api.event.base.Event(
        topic='mroya.asset.watch',
        data={
            'asset_id': 'asset-uuid',
            'asset_name': 'BigMan',
            'component_name': 'anim.fbx',  # or None for all components
            'component_id': 'comp-uuid',
            'target_location_id': 'burlin.local',
            'current_version_id': 'version-uuid',
            'source_dcc': 'houdini',
            'scene_path': '/path/to/scene.hip',
            'auto_transfer': True,
            'notify_dcc': True,
        }
    ),
    on_error='ignore'
)

# Stop watching
session.event_hub.publish(
    ftrack_api.event.base.Event(
        topic='mroya.asset.unwatch',
        data={
            'asset_id': 'asset-uuid',
            'component_name': 'anim.fbx',
        }
    ),
    on_error='ignore'
)
```

### Connect UI

The Asset Watcher tab in ftrack Connect shows:
- List of watched assets
- Current/pending version info
- Status (watching, update_available, transferring)
- Target location
- Source DCC
- Actions (Transfer, Remove)

## Events

### Incoming (from DCC)

- `mroya.asset.watch` - Add asset to watchlist
- `mroya.asset.unwatch` - Remove asset from watchlist

### Outgoing (to DCC)

- `mroya.asset.update-available` - New version available (TODO)
- `mroya.asset.transfer-complete` - Transfer finished (TODO)

## Storage

Watchlist is stored in `~/.ftrack/mroya_asset_watcher.json`

## Configuration

Settings in watchlist file:
```json
{
  "settings": {
    "auto_transfer": true,
    "notify_dcc": true,
    "check_interval_seconds": 60
  }
}
```
