import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# Simulated data: (time, lat, lon)
time_steps = 20
lat_size = 90
lon_size = 180
data = np.random.rand(time_steps, lat_size, lon_size)

# Latitude and longitude grids
lats = np.linspace(-90, 90, lat_size)
lons = np.linspace(-180, 180, lon_size)
lon_grid, lat_grid = np.meshgrid(lons, lats)

# Set up the figure and map
fig = plt.figure(figsize=(10, 5))
ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_global()
ax.coastlines()
ax.add_feature(cfeature.BORDERS, linestyle=':')
ax.gridlines(draw_labels=True)

# Initial plot
cax = ax.pcolormesh(lon_grid, lat_grid, data[0], transform=ccrs.PlateCarree(), cmap='viridis')
cb = plt.colorbar(cax, orientation='horizontal', pad=0.05)
cb.set_label('Important value')

# Animation update function
def update(frame):
    cax.set_array(data[frame].flatten())
    ax.set_title(f"Time Step: {frame}")
    return cax,

ani = animation.FuncAnimation(fig, update, frames=range(time_steps), blit=False, repeat=True)
plt.show()

