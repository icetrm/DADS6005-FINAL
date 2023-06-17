
#!/usr/bin/env python3
"""
Name:			Space Classes
Author:			Dan Petersson
Github link:	https://github.com/DanPetersson/SpaceWars

Description:
---------------------------------------------------------
- Classes for Pygame "Space Objects" that can be used for space games
- Used by "Space Wars" game

---------------------------------------------------------
Python:			3.8 						
PyGame:			1.9.6

Revision updates:
---------------------------------------------------------
Backlog_revision_history.txt
"""

# initialize pygame 
import pygame
pygame.init()

# Initialize global fonts
font_huge	= pygame.font.Font('freesansbold.ttf', 128)
font_large	= pygame.font.Font('freesansbold.ttf', 64)
font_medium	= pygame.font.Font('freesansbold.ttf', 32)
font_small	= pygame.font.Font('freesansbold.ttf', 16)
font_tiny	= pygame.font.Font('freesansbold.ttf', 8)

# Initialize global Game Colors
black 	= (  0,   0,   0)
white 	= (255, 255, 255)

red 	= (200,   0,   0)
green 	= (  0, 200,   0)
blue 	= (  0,   0, 200)
yellow 	= (200, 200,   0)

light_red 		= (255,   0,   0)
Light_green 	= (  0, 255,   0)
light_blue 		= (  0,   0, 255)
light_yellow	= (255, 255,   0)

# ----------------------------
# 		Define Classes
# ----------------------------

class SpaceObject:

	def __init__(self, image, explosion_image, posX=0, posY=0, speedX = 0, speedY = 0, sizeX = 64, sizeY = 64, 
					state = 'show', sound = ' ', hit_points = 1):
		#self.namme	= name
		self.image  = image
		self.explosion_image = explosion_image 
		self.sizeX  = sizeX
		self.sizeY  = sizeY
		self.posX   = posX
		self.posY   = posY
		self.speedX = speedX
		self.speedY	= speedY
		self.state	= state		# 'hide', 'show'
		self.sound 	= sound
		self.explosion_counter = -1
		self.hit_points = hit_points

	def show(self):
		if self.state == 'show' and self.explosion_counter <= 0:
			screen.blit(self.image, (int(self.posX), int(self.posY)))
		elif self.explosion_counter > 0:
			screen.blit(self.explosion_image, (int(self.posX), int(self.posY)))
			
class SpaceShip(SpaceObject):
    
    # def __init__(self):
    #     super().__init__()

	def update_player_postion(self, screen_sizeX, screen_sizeY):

		# Update X position (update with min/max)
		self.posX += self.speedX
		if self.posX < 0:
			self.posX = 0
		elif self.posX > screen_sizeX-self.sizeX:
			self.posX = screen_sizeX-self.sizeX

		# Update Y position (update with min/max)
		self.posY += self.speedY
		if self.posY < 0:
			self.posY = 0
		elif self.posY > screen_sizeY-self.sizeY:
			self.posY = screen_sizeY-self.sizeY


class SpaceEnemy(SpaceObject):

	def update_enemy_position(self, screen_sizeX, screen_sizeY):

		# Update X position
		self.posX += self.speedX

		# Update Y position
		self.posY += self.speedY

class Bullet(SpaceObject):

	def update_bullet_position(self, screen_sizeX, screen_sizeY):

		# Update X position
		self.posX += self.speedX

		# Update Y position, and change state if outside screen
		self.posY += self.speedY
		if self.posY < -self.sizeY:
			self.state = 'hide'


	def fire_bullet(self, player):

		self.posX = player.posX + player.sizeX/2 - self.sizeX/2
		self.posY = player.posY
		self.sound.play()
		self.state = 'show'


class Button:

	def __init__(self, centerX, centerY, width, hight, text='', color=yellow, color_hoover=light_yellow, 
		text_color=black, text_hoover=black, font=font_small):
		self.centerX 		= int(centerX)
		self.centerY		= int(centerY)
		self.width 			= int(width)
		self.hight 			= int(hight)
		self.X				= int(centerX - width/2)
		self.Y				= int(centerY - hight/2)

		self.text 			= text
		self.color 			= color
		self.color_hoover	= color_hoover
		self.text_color		= text_color
		self.text_hoover	= text_hoover
		self.font 			= font
		self.clicked		= False

	# internal only function ?
	def text_objects(text, font, color):
	    text_surface = font.render(text, True, color)
	    return text_surface, text_surface.get_rect()

	# internal only function ?
	def message_display(text, font, color, centerX, centerY):
	    text_surface, text_rectangle = text_objects(text, font, color)
	    text_rectangle.center = (centerX,centerY)
	    screen.blit(text_surface, text_rectangle)

	def show(self, screen, mouse=(0,0)):
		if self.X < mouse[0] < self.X + self.width and self.Y < mouse[1] < self.Y + self.hight:
			pygame.draw.rect(screen, self.color_hoover, (self.X, self.Y, self.width, self.hight))
		else:
			pygame.draw.rect(screen, yellow, (self.X, self.Y, self.width, self.hight))
		message_display(self.text, self.font, black, self.centerX, self.centerY)

	def check_clicked(self, mouse, mouse_click):
		if self.X < mouse[0] < self.X + self.width and self.Y < mouse[1] < self.Y + self.hight and mouse_click[0] == 1:
			self.clicked = True
		else:
			self.clicked = False



